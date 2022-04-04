using jessielesbian.OpenCEX.SafeMath;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;

namespace jessielesbian.OpenCEX{
	public sealed class SQLCommandFactory : IDisposable
	{
		private readonly MySqlConnection mySqlConnection;
		private MySqlTransaction mySqlTransaction;
		private bool disposedValue;
		private MySqlDataReader dataReader = null;

		public SQLCommandFactory(MySqlConnection mySqlConnection, MySqlTransaction mySqlTransaction)
		{
			this.mySqlConnection = mySqlConnection ?? throw new ArgumentNullException(nameof(mySqlConnection));
			this.mySqlTransaction = mySqlTransaction ?? throw new ArgumentNullException(nameof(mySqlTransaction));
		}

		public MySqlDataReader SafeExecuteReader(MySqlCommand mySqlCommand){
			StaticUtils.CheckSafety2(dataReader, "Data reader already created!");
			MySqlDataReader temp = mySqlCommand.ExecuteReader();
			temp.Read();
			dataReader = temp;
			return temp;
		}

		public void SafeDestroyReader(){
			StaticUtils.CheckSafety(dataReader, "Data reader already destroyed!");
			dataReader.Close();
			dataReader = null;
		}

		private void RequireTransaction()
		{
			StaticUtils.CheckSafety2(disposedValue, "MySQL connection already disposed!");
			StaticUtils.CheckSafety(mySqlTransaction, "MySQL transaction not open!");
		}

		public MySqlCommand GetCommand(string cmd)
		{
			RequireTransaction();
			return new MySqlCommand(cmd, mySqlConnection, mySqlTransaction);
		}

		public void DestroyTransaction(bool commit, bool destroy)
		{
			RequireTransaction();
			try
			{
				if (commit)
				{
					StaticUtils.CheckSafety2(dataReader, "Data reader still open!");

					
					Queue<KeyValuePair<string, SafeUint>> pendingFlush = new Queue<KeyValuePair<string, SafeUint>>();
					foreach(KeyValuePair<string, SafeUint> balanceUpdate in dirtyBalances){
						//Flush dirty balances
						string key = balanceUpdate.Key;
						MySqlCommand command = GetCommand(balanceUpdateCommands[key]);
						command.Parameters.AddWithValue("@balance", balanceUpdate.Value.ToString());
						command.Parameters.AddWithValue("@coin", key.Substring(key.IndexOf('_') + 1));
						command.Prepare();
						command.SafeExecuteNonQuery();

						//Prepare to write to cache
						pendingFlush.Enqueue(balanceUpdate);
					}

					try{
						while (pendingFlush.TryDequeue(out KeyValuePair<string, SafeUint> result))
						{
							L3BalancesCache[result.Key].Value = result.Value;
						}

						//Release balance cache locks
						while (release.TryDequeue(out PooledManualResetEvent result))
						{
							result.Set();
						}
					} catch (Exception e){
						Console.Error.WriteLine("Clearing balances cache due to exception: " + e.ToString());
						lock(L3Blacklist){
							L3BalancesCache.Clear();
							L3Blacklist.Clear();
						}
					}
					
					mySqlTransaction.Commit();
					mySqlTransaction = null;
				}
				else
				{
					try{
						if (dataReader != null)
						{
							SafeDestroyReader();
						}
						mySqlTransaction.Rollback();
						mySqlTransaction.Dispose();
					} finally{
						mySqlTransaction = null;
					}
					
				}

				if (destroy)
				{
					Dispose();
				}

			}
			catch (Exception e)
			{
				if (StaticUtils.debug)
				{
					throw new SafetyException("Unable to destroy MySQL transaction!", e);
				}
				else
				{
					throw new SafetyException("Unable to destroy MySQL transaction!");
				}

			}
		}

		/// <summary>
		/// Executes query and restricts effect to single row
		/// </summary>
		public void SafeExecuteNonQuery(string query){
			StaticUtils.CheckSafety(GetCommand(query).ExecuteNonQuery() == 1, "Excessive write effect (should not reach here)!", true);
		}

		public void BeginTransaction()
		{
			StaticUtils.CheckSafety2(disposedValue, "MySQL connection already disposed!");
			StaticUtils.CheckSafety2(mySqlTransaction, "MySQL transaction already exist!");
			mySqlTransaction = mySqlConnection.BeginTransaction();
		}

		public void Dispose()
		{
			GC.SuppressFinalize(this);
			if (!disposedValue)
			{
				if (mySqlTransaction != null)
				{
					DestroyTransaction(false, true);
				}
				else
				{
					mySqlConnection.Close();

					// TODO: free unmanaged resources (unmanaged objects) and override finalizer
					// TODO: set large fields to null
					disposedValue = true;
				}
			}
		}

		private readonly Dictionary<string, SafeUint> cachedBalances = new Dictionary<string, SafeUint>();
		private readonly Dictionary<string, SafeUint> dirtyBalances = new Dictionary<string, SafeUint>();
		private readonly Dictionary<string, string> balanceUpdateCommands = new Dictionary<string, string>();
		private static volatile int balancesCacheCounter = 0;
		private sealed class L3Balance{
			private SafeUint balance;
			private int counter;
			public SafeUint Value {
				get{
					counter = Interlocked.Increment(ref balancesCacheCounter);
					return balance;
				}
				set{
					counter = Interlocked.Increment(ref balancesCacheCounter);
					balance = value;
				}
			}
			public int Counter => counter;

			public readonly PooledManualResetEvent syncer = PooledManualResetEvent.GetInstance(true);

			public L3Balance(SafeUint value)
			{
				balance = value ?? throw new ArgumentNullException(nameof(value));
				counter = Interlocked.Increment(ref balancesCacheCounter);
			}
		}
		private static readonly ConcurrentDictionary<string, L3Balance> L3BalancesCache = new ConcurrentDictionary<string, L3Balance>();
		private static readonly ConcurrentDictionary<string, int> L3Blacklist = new ConcurrentDictionary<string, int>();
		private Queue<PooledManualResetEvent> release = new Queue<PooledManualResetEvent>();

		public SafeUint GetBalance(string coin, ulong userid){
			string key = userid + "_" + coin;
			SafeUint balance;
			if (dirtyBalances.TryGetValue(key, out balance))
			{
				return balance;
			}
			else if(cachedBalances.TryGetValue(key, out balance)){
				return balance;
			}
			else if (StaticUtils.Multiserver)
			{
				return FetchBalanceIMPL(key);
			}
			else
			{

				bool newcache = true;
				lock (evlock)
				{
					byte[] rngbuffer = new byte[4];
					if (cachedBalances.Count > StaticUtils.MaximumBalanceCacheSize)
					{
						RandomNumberGenerator randomNumberGenerator = RandomNumberGenerator.Create();
						ICollection<string> keys = L3BalancesCache.Keys;
						string[] k2 = new string[keys.Count];
						keys.CopyTo(k2, 0);

						randomNumberGenerator.GetBytes(rngbuffer);
						int limit = StaticUtils.MaximumBalanceCacheSize > 16 ? (StaticUtils.MaximumBalanceCacheSize / 16) : 1;
						string evict = null;
						int oldest = int.MaxValue;
						PooledManualResetEvent dispose = null;

						//Hybrid LRU/RR cache eviction
						for (int i = 0; i < limit; ++i)
						{
							string key2 = k2[BitConverter.ToUInt32(rngbuffer, 0) % ((uint)keys.Count)];
							if(L3BalancesCache.TryGetValue(key2, out L3Balance l3balance)){
								int ctr = l3balance.Counter;
								if (ctr <= oldest)
								{
									evict = key2;
									oldest = ctr;
									dispose = l3balance.syncer;
								}
							} else{
								++limit;
							}
						}

						if(evict != null){
							lock(L3Blacklist){
								if(L3BalancesCache.ContainsKey(evict)){
									StaticUtils.CheckSafety(L3Blacklist.TryAdd(evict, 0), "Unable to blacklist balance from cache (should not reach here)!", true);
									dispose.Wait3();
									dispose.Dispose();
								} else{
									newcache = false;
									goto noremove;
								}
							}
							
							newcache = L3BalancesCache.TryRemove(evict, out _);
							StaticUtils.CheckSafety(L3Blacklist.TryRemove(evict, out _), "Unable to remove balances cache blacklisting (should not reach here)!", true);
						}
						noremove:

						randomNumberGenerator.Dispose();
					}
					
				}
				lock (L3Blacklist){
					if (L3Blacklist.ContainsKey(key))
					{
						return FetchBalanceIMPL(key);
					}
					else
					{
						L3Balance l3 = L3BalancesCache.GetOrAdd(key, FetchBalance2);
						l3.syncer.Wait2();
						release.Enqueue(l3.syncer);
						return l3.Value;
					}
				}
			}
		}

		private static readonly object evlock = new object();
		private SafeUint FetchBalanceIMPL(string key){
			int pivot = key.IndexOf('_');
			string userid = key.Substring(0, pivot);

			//Fetch balance from database
			MySqlCommand command = GetCommand("SELECT Balance FROM Balances WHERE UserID = " + userid + " AND Coin = @coin FOR UPDATE;");
			command.Parameters.AddWithValue("@coin", key.Substring(pivot + 1));
			command.Prepare();
			MySqlDataReader reader = SafeExecuteReader(command);
			SafeUint balance;
			if (reader.HasRows)
			{
				balance = StaticUtils.GetSafeUint(reader.GetString("Balance"));
				reader.CheckSingletonResult();
				balanceUpdateCommands.Add(key, "UPDATE Balances SET Balance = @balance WHERE UserID = " + userid + " AND Coin = @coin;");
			}
			else
			{
				balance = StaticUtils.zero;
				balanceUpdateCommands.Add(key, "INSERT INTO Balances (Balance, UserID, Coin) VALUES (@balance, " + userid + ", @coin);");
			}

			SafeDestroyReader();
			cachedBalances.Add(key, balance);
			return balance;
		}

		private L3Balance FetchBalance2(string key)
		{
			return new L3Balance(FetchBalanceIMPL(key));
		}

		public void UpdateBalance(string coin, ulong userid, SafeUint balance)
		{
			string key = userid + "_" + coin;
			if(cachedBalances.TryAdd(key, balance))
			{
				StaticUtils.CheckSafety(L3BalancesCache.ContainsKey(key), "Attempted to update uncached balance (should not reach here)!", true);
				StaticUtils.CheckSafety(balanceUpdateCommands.TryAdd(key, "UPDATE Balances SET Balance = @balance WHERE UserID = " + userid + " AND Coin = @coin;"), "Balance update command already defined (should not reach here)!", true);
			}

			if (!dirtyBalances.TryAdd(key, balance))
			{
				dirtyBalances[key] = balance;
			}
		}

		~SQLCommandFactory()
		{
			Dispose();
		}
	}

	public static partial class StaticUtils{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void SafeExecuteNonQuery(this MySqlCommand mySqlCommand)
		{
			CheckSafety(mySqlCommand.ExecuteNonQuery() == 1, "Excessive write effect (should not reach here)!", true);
		}
	}
}
