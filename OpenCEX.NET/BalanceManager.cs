using jessielesbian.OpenCEX.RequestManager;
using jessielesbian.OpenCEX.SafeMath;
using MySql.Data.MySqlClient;
using System.Numerics;

namespace jessielesbian.OpenCEX{
	public static class BalanceManager{		
		private static void CreditOrDebit(SQLCommandFactory sqlCommandFactory, string coin, ulong userid, SafeUint amount, bool credit){
			//Fetch balance from database
			MySqlCommand command = sqlCommandFactory.GetCommand("SELECT Balance FROM Balances WHERE UserID = " + userid + " AND Coin = @coin FOR UPDATE;");
			command.Prepare();
			command.Parameters.AddWithValue("@coin", coin);
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
				balance = balance.Sub(amount, "Insufficent balance!");
			}

			//Fill in SQL query
			command.Prepare();
			command.Parameters.AddWithValue("@coin", coin);
			command.Parameters.AddWithValue("@balance", balance.ToString());

			//Execute SQL query
			command.ExecuteNonQuery();
		}

		/// <summary>
		/// Credit funds to a customer account.
		/// </summary>
		public static void Credit(this SQLCommandFactory sqlCommandFactory, string coin, ulong userid, SafeUint amount){
			CreditOrDebit(sqlCommandFactory, coin, userid, amount, true);
		}

		/// <summary>
		/// Debit funds from a customer account.
		/// </summary>
		public static void Debit(this SQLCommandFactory sqlCommandFactory, string coin, ulong userid, SafeUint amount)
		{
			CreditOrDebit(sqlCommandFactory, coin, userid, amount, false);
		}

		/// <summary>
		/// Credit funds to a customer account.
		/// </summary>
		public static void Credit(this Request request, string coin, ulong userid, SafeUint amount)
		{
			CreditOrDebit(request.sqlCommandFactory, coin, userid, amount, true);
		}

		/// <summary>
		/// Debit funds from a customer account.
		/// </summary>
		public static void Debit(this Request request, string coin, ulong userid, SafeUint amount)
		{
			CreditOrDebit(request.sqlCommandFactory, coin, userid, amount, false);
		}
	}
}