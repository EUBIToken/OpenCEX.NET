using jessielesbian.OpenCEX.RequestManager;
using jessielesbian.OpenCEX.SafeMath;
using MySql.Data.MySqlClient;
using System;
using System.Numerics;

namespace jessielesbian.OpenCEX{
	public static class BalanceManager{		
		private static void CreditOrDebit(SQLCommandFactory sqlCommandFactory, string coin, ulong userid, SafeUint amount, bool credit)
		{
			SafeUint balance = sqlCommandFactory.GetBalance(coin, userid);
			if(credit){
				balance = balance.Add(amount);
			} else{
				balance = balance.Sub(amount, "Insufficent balance!", false);
			}
			sqlCommandFactory.UpdateBalance(coin, userid, balance);
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