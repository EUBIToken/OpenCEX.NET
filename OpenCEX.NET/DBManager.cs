using jessielesbian.OpenCEX.SafeMath;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Numerics;
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
		private readonly MySqlCommand readBalance;

		public SQLCommandFactory(MySqlConnection mySqlConnection, MySqlTransaction mySqlTransaction)
		{
			this.mySqlConnection = mySqlConnection ?? throw new ArgumentNullException(nameof(mySqlConnection));
			this.mySqlTransaction = mySqlTransaction ?? throw new ArgumentNullException(nameof(mySqlTransaction));
			readBalance = GetCommand("SELECT Balance FROM Balances WHERE Coin = @coin AND UserID = @userid");
			readBalance.Parameters.AddWithValue("@coin", string.Empty);
			readBalance.Parameters.AddWithValue("@userid", 0UL);
			readBalance.Prepare();
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

		private ConcurrentJob postCommit = null;
		internal void AfterCommit(ConcurrentJob postCommit){
			StaticUtils.CheckSafety2(this.postCommit, "Post-commit task defined twice (should not reach here)!", true);
			this.postCommit = postCommit ?? throw new ArgumentNullException(nameof(postCommit));
		}

		public void DestroyTransaction(bool commit, bool destroy)
		{
			RequireTransaction();
			try
			{
				if (commit)
				{
					StaticUtils.CheckSafety2(dataReader, "Data reader still open!");
					Queue<KeyValuePair<string, SafeUint>> pendingFlush;
					bool doflush2;
					int limit3 = netBalanceEffects.Count;
					if (limit3 == 0)
					{
						pendingFlush = null;
						doflush2 = false;
					} else{
						MySqlCommand update = GetCommand("UPDATE Balances SET Balance = @balance WHERE UserID = @userid AND Coin = @coin AND Balance = @old;");
						update.Parameters.AddWithValue("@balance", string.Empty);
						update.Parameters.AddWithValue("@userid", 0UL);
						update.Parameters.AddWithValue("@coin", string.Empty);
						update.Parameters.AddWithValue("@old", string.Empty);
						update.Prepare();

						MySqlCommand insert = GetCommand("INSERT INTO Balances(Balance, UserID, Coin) VALUES (@balance, @userid, @coin);");
						insert.Parameters.AddWithValue("@balance", string.Empty);
						insert.Parameters.AddWithValue("@userid", 0UL);
						insert.Parameters.AddWithValue("@coin", string.Empty);
						insert.Prepare();

						string[] order;
						{
							List<string> updates = new List<string>(limit3);
							foreach (KeyValuePair<string, BigInteger> keyValuePair in netBalanceEffects)
							{
								updates.Add(keyValuePair.Key);
							}

							updates.Sort();

							for (int i = 0; i < limit3; i++)
							{

							}

							order = updates.ToArray();
						}
						
						for(int i = 0; i < limit3; ){
							string key = order[i++];
							StaticUtils.CheckSafety(netBalanceEffects.TryGetValue(key, out BigInteger bigInteger), "Unable to retrieve net effects key (should not reach here)!", true);
							int pivot = key.IndexOf('_');
							int sign = bigInteger.Sign;
							if(sign > 0){
								CreditOrDebit(key.Substring(pivot + 1), Convert.ToUInt64(key.Substring(0, pivot)), new SafeUint(bigInteger), true);
							} else if(sign < 0){
								CreditOrDebit(key.Substring(pivot + 1), Convert.ToUInt64(key.Substring(0, pivot)), new SafeUint(bigInteger * BigInteger.MinusOne), false);
							}
						}

						pendingFlush = new Queue<KeyValuePair<string, SafeUint>>();
						foreach (KeyValuePair<string, SafeUint> balanceUpdate in dirtyBalances)
						{
							//Flush dirty balances
							string key = balanceUpdate.Key;
							int pivot = key.IndexOf('_');

							MySqlCommand command;
							if (OriginalBalances.TryGetValue(key, out string oldbal))
							{
								command = update;
								command.Parameters["@old"].Value = oldbal;
							}
							else
							{
								command = insert;
							}

							command.Parameters["@balance"].Value = balanceUpdate.Value.ToString();
							command.Parameters["@userid"].Value = Convert.ToUInt64(key.Substring(0, pivot));
							command.Parameters["@coin"].Value = key.Substring(pivot + 1);

							try
							{
								command.SafeExecuteNonQuery();
							}
							catch (Exception e)
							{
								Console.Error.WriteLine("Invalidating balances cache entry due to exception: " + e.ToString());
								L3BalancesCache2.Clear(key);
								throw e;
							}

							//Prepare to write to cache
							pendingFlush.Enqueue(balanceUpdate);
						}
						doflush2 = true;
					}


					mySqlTransaction.Commit();
					mySqlTransaction = null;

					if (postCommit != null)
					{
						StaticUtils.Append(postCommit);
						postCommit = null;
					}
					if(doflush2){
						while (pendingFlush.TryDequeue(out KeyValuePair<string, SafeUint> result))
						{
							try{
								L3BalancesCache2.UpdateOrAdd(result.Key, result.Value);
								
							} catch (Exception e){
								Console.Error.WriteLine("Invalidating balances cache entry due to exception: " + e.ToString());
							}
						}

						//Release balances cache locks (battle override active)
						while (pendingLockRelease.TryDequeue(out string result))
						{
							try
							{
								L3BalancesCache2.Unlock(result);
							}
							catch (Exception e)
							{
								Console.Error.WriteLine("Error while unlocking balances cache: " + e.ToString());
							}
						}
					}	
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

				dirtyBalances.Clear();

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

		private readonly Dictionary<string, BigInteger> netBalanceEffects = new Dictionary<string, BigInteger>();
		private readonly Dictionary<string, SafeUint> dirtyBalances = new Dictionary<string, SafeUint>();
		private readonly Dictionary<string, string> OriginalBalances = new Dictionary<string, string>();
		private static readonly ConcurrentDualGenerationCache<string, SafeUint> L3BalancesCache2 = new ConcurrentDualGenerationCache<string, SafeUint>(StaticUtils.MaximumBalanceCacheSize);
		private readonly Queue<string> pendingLockRelease = new Queue<string>();

		/// <summary>
		/// Should not be called directly
		/// </summary>
		public SafeUint GetBalance(string coin, ulong userid){
			string key = userid + "_" + coin;
			if (StaticUtils.Multiserver)
			{
				return FetchBalanceIMPL(key);
			}
			else
			{
				//Battle override C# safety
				try{
					L3BalancesCache2.LockWrite(key);
				} finally{
					pendingLockRelease.Enqueue(key);
				}
				
				SafeUint balance = L3BalancesCache2.GetOrAdd(key, FetchBalanceIMPL, out bool called);
				if(!called)
				{
					StaticUtils.CheckSafety(OriginalBalances.TryAdd(key, balance.ToString()), "Unable to register balances caching safety check (should not reach here)!", true);
				}
				return balance;
			}
		}

		private SafeUint FetchBalanceIMPL(string key){
			int pivot = key.IndexOf('_');

			//Fetch balance from database
			readBalance.Parameters["@userid"].Value = Convert.ToUInt64(key.Substring(0, pivot));
			readBalance.Parameters["@coin"].Value = key.Substring(pivot + 1);
			MySqlDataReader reader = SafeExecuteReader(readBalance);
			SafeUint balance;
			if (reader.HasRows)
			{
				balance = StaticUtils.GetSafeUint(reader.GetString("Balance"));
				reader.CheckSingletonResult();
				StaticUtils.CheckSafety(OriginalBalances.TryAdd(key, balance.ToString()), "Unable to register balances caching safety check (should not reach here)!", true);
			}
			else
			{
				balance = StaticUtils.zero;
			}

			SafeDestroyReader();
			return balance;
		}

		public void UpdateBalance(string coin, ulong userid, SafeUint balance)
		{
			string key = userid + "_" + coin;

			if (!dirtyBalances.TryAdd(key, balance))
			{
				dirtyBalances[key] = balance;
			}
		}

		private void CreditOrDebit(string coin, ulong userid, SafeUint amount, bool credit)
		{
			SafeUint balance = GetBalance(coin, userid);
			if (credit)
			{
				balance = balance.Add(amount);
			}
			else
			{
				balance = balance.Sub(amount, "Insufficent balance!", false);
			}
			UpdateBalance(coin, userid, balance);
		}

		private void ShiftBalance(string key, BigInteger bigInteger){
			if(netBalanceEffects.TryGetValue(key, out BigInteger balanceEffect)){
				netBalanceEffects[key] = balanceEffect + bigInteger;
			} else{
				StaticUtils.CheckSafety(netBalanceEffects.TryAdd(key, bigInteger), "Unable to add balance to effects optimization cache (should not reach here)!", true);
			}
		}

		/// <summary>
		/// Credit funds to a customer account.
		/// </summary>
		public void Credit(string coin, ulong userid, SafeUint amount, bool safe = true)
		{
			StaticUtils.CheckSafety2(userid == 0, "Unexpected credit to null account!");
			if (safe)
			{
				//Shortfall protection: debit funds from null account
				ShiftBalance("0_" + coin, BigInteger.Negate(amount.bigInteger));
			}
			ShiftBalance(userid.ToString() + '_' + coin, amount.bigInteger);
		}

		/// <summary>
		/// Debit funds from a customer account.
		/// </summary>
		public void Debit(string coin, ulong userid, SafeUint amount, bool safe = true)
		{
			StaticUtils.CheckSafety2(userid == 0, "Unexpected debit from null account!");
			if (safe)
			{
				//Shortfall protection: debit funds from null account
				ShiftBalance("0_" + coin, amount.bigInteger);
			}
			ShiftBalance(userid.ToString() + '_' + coin, BigInteger.Negate(amount.bigInteger));
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
			int delta = mySqlCommand.ExecuteNonQuery();
			CheckSafety2(delta == 0, "Insufficent write effect (should not reach here)!", true);
			CheckSafety2(delta > 1, "Excessive write effect (should not reach here)!", true);
		}
	}
}
