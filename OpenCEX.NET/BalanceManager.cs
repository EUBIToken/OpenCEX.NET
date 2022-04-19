using jessielesbian.OpenCEX.RequestManager;
using jessielesbian.OpenCEX.SafeMath;

namespace jessielesbian.OpenCEX{
	public static class BalanceManager{

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