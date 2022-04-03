using jessielesbian.OpenCEX.SafeMath;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;

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
					foreach(KeyValuePair<string, SafeUint> balanceUpdate in dirtyBalances){
						//Flush dirty balances
						string key = balanceUpdate.Key;
						MySqlCommand command = GetCommand(balanceUpdateCommands[key]);
						command.Parameters.AddWithValue("@balance", balanceUpdate.Value.ToString());
						command.Parameters.AddWithValue("@coin", key.Substring(key.IndexOf('_') + 1));
						command.Prepare();
						command.ExecuteNonQuery();
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

		public SafeUint GetBalance(string coin, ulong userid){
			string key = userid + '_' + coin;
			SafeUint balance;
			if (dirtyBalances.TryGetValue(key, out balance))
			{
				return balance;
			}
			else if(cachedBalances.TryGetValue(key, out balance)){
				return balance;
			} else{
				//Fetch balance from database
				MySqlCommand command = GetCommand("SELECT Balance FROM Balances WHERE UserID = " + userid + " AND Coin = @coin FOR UPDATE;");
				command.Parameters.AddWithValue("@coin", coin);
				command.Prepare();
				MySqlDataReader reader = SafeExecuteReader(command);
				
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
		}

		public void UpdateBalance(string coin, ulong userid, SafeUint balance)
		{
			string key = userid + '_' + coin;
			StaticUtils.CheckSafety(cachedBalances.TryGetValue(key, out SafeUint temp), "Update uncached balance (should not reach here)!", true);
			if(balance == temp){
				dirtyBalances.Remove(key); //Restore original balance
			} else
			{
				if(!dirtyBalances.TryAdd(key, balance)){
					dirtyBalances[key] = balance;
				}
			}
		}

		~SQLCommandFactory()
		{
			Dispose();
		}
	}

	public static partial class StaticUtils{
		public static void SafeExecuteNonQuery(this MySqlCommand mySqlCommand)
		{
			CheckSafety(mySqlCommand.ExecuteNonQuery() == 1, "Excessive write effect (should not reach here)!", true);
		}
	}
}
