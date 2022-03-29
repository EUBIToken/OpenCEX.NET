using Nethereum.Util;
using System;

namespace jessielesbian.OpenCEX
{
	public static partial class StaticUtils
	{
		private static readonly AddressUtil addressUtil = new AddressUtil();
		public static void VerifyAddress(string address){
			CheckSafety(address.Length == 42 && address.StartsWith("0x") && addressUtil.IsValidEthereumAddressHexFormat(address), "Invalid address!");
		}
		public static string ExpandABIAddress(string address)
		{
			VerifyAddress(address);
			char[] temp = address.ToCharArray();
			temp[1] = '0';
			return "0000000000000000000000" + new string(temp);
		}
	}
}