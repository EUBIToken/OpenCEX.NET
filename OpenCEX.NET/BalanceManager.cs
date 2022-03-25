using jessielesbian.OpenCEX.RequestManager;
using jessielesbian.OpenCEX.SafeMath;
using MySql.Data.MySqlClient;
using System;
using System.Numerics;

namespace jessielesbian.OpenCEX{
	public static class BalanceManager{		
		private static void CreditOrDebit(SQLCommandFactory sqlCommandFactory, string coin, ulong userid, SafeUint amount, bool credit)
		{
			//Fetch balance from database
			MySqlCommand command = sqlCommandFactory.GetCommand("SELECT Balance FROM Balances WHERE UserID = " + userid + " AND Coin = @coin FOR UPDATE;");
			command.Parameters.AddWithValue("@coin", coin);
			command.Prepare();
			MySqlDataReader reader = sqlCommandFactory.SafeExecuteReader(command);
			SafeUint balance;
			if (reader.HasRows)
			{
				balance = StaticUtils.GetSafeUint(reader.GetString("Balance"));
				reader.CheckSingletonResult();
				command = sqlCommandFactory.GetCommand("UPDATE Balances SET Balance = @balance WHERE UserID = " + userid + " AND Coin = @coin;");
			}
			else
			{
				balance = new SafeUint(BigInteger.Zero);
				command = sqlCommandFactory.GetCommand("INSERT INTO Balances (Balance, UserID, Coin) VALUES (@balance, " + userid + ", @coin);");
			}
			sqlCommandFactory.SafeDestroyReader();

			//Manipulate balance with safety checks
			if (credit){
				balance = balance.Add(amount);
			} else{
				if (userid == 0)
				{
					if (balance < amount) {
						try{
							throw new SafetyException("SHORTFALL DETECTED: " + balance.ToString() + " - " + amount + "!");
						} catch (Exception e){
							Console.Error.WriteLine(e);
							throw e;
						}
					}
					balance = balance.Sub(amount);
				} else{
					balance = balance.Sub(amount, "Insufficent balance!");
				}
				
			}

			//Fill in SQL query
			command.Parameters.AddWithValue("@coin", coin);
			command.Parameters.AddWithValue("@balance", balance.ToString());
			command.Prepare();

			//Execute SQL query
			command.ExecuteNonQuery();
		}

		/// <summary>
		/// Credit funds to a customer account.
		/// </summary>
		public static void Credit(this SQLCommandFactory sqlCommandFactory, string coin, ulong userid, SafeUint amount, bool safe = true){
			StaticUtils.CheckSafety2(userid == 0, "Unexpected credit to null account!");
			if(safe){
				//Shortfall protection: debit funds from null account
				CreditOrDebit(sqlCommandFactory, coin, 0, amount, false);
			}
			CreditOrDebit(sqlCommandFactory, coin, userid, amount, true);
		}

		/// <summary>
		/// Debit funds from a customer account.
		/// </summary>
		public static void Debit(this SQLCommandFactory sqlCommandFactory, string coin, ulong userid, SafeUint amount, bool safe = true)
		{
			StaticUtils.CheckSafety2(userid == 0, "Unexpected debit from null account!");
			if (safe){
				//Shortfall protection: credit funds to null account
				CreditOrDebit(sqlCommandFactory, coin, 0, amount, true);
			}
			CreditOrDebit(sqlCommandFactory, coin, userid, amount, false);
		}

		/// <summary>
		/// Credit funds to a customer account.
		/// </summary>
		public static void Credit(this Request request, string coin, ulong userid, SafeUint amount, bool safe = true)
		{
			request.sqlCommandFactory.Credit(coin, userid, amount, safe);
		}

		/// <summary>
		/// Debit funds from a customer account.
		/// </summary>
		public static void Debit(this Request request, string coin, ulong userid, SafeUint amount, bool safe = true)
		{
			request.sqlCommandFactory.Debit(coin, userid, amount, safe);
		}
	}
}