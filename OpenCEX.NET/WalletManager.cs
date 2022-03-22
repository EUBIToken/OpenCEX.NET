using System;
using Nethereum.Web3;
using Nethereum.Web3.Accounts;
namespace jessielesbian.OpenCEX
{
	public static partial class StaticUtils
	{
		
		private static readonly Account masterWallet;
		
	}
	public sealed class BlockchainManager{
		public readonly string node;
		public readonly ulong chainid;
		public static readonly BlockchainManager MintME = new BlockchainManager("https://node" + ((DateTimeOffset.Now.ToUnixTimeMilliseconds() % 2) + 1).ToString() + ".mintme.com", 24734);

		public static readonly BlockchainManager Polygon = new BlockchainManager("https://polygon-rpc.com", 137);
		public static readonly BlockchainManager BinanceSmartChain = new BlockchainManager("https://bscrpc.com", 56);
		private BlockchainManager(string node, ulong chainid)
		{
			this.node = node ?? throw new ArgumentNullException(nameof(node));
			this.chainid = chainid;
		}
		public Web3 GetWeb3(string privateKey){
			return new Web3(new Account(privateKey, chainid));
		}
	}
}

