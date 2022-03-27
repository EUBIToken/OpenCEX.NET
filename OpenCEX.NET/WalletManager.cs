using System;
using System.Collections.Concurrent;
using Nethereum.Contracts.Services;
using Nethereum.Web3;
using Nethereum.Web3.Accounts;
using System.Security.Cryptography;
using System.Text;
using Nethereum.JsonRpc.Client;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using jessielesbian.OpenCEX.SafeMath;
using Nethereum.Hex.HexTypes;
using Nethereum.RPC.TransactionManagers;
using System.Numerics;
using System.Collections.Generic;
using System.Net;
using System.Web;
using System.IO;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace jessielesbian.OpenCEX
{
	public static partial class StaticUtils
	{
		public static readonly string ExchangeWalletAddress = BlockchainManager.MintME.GetWalletManager(GetEnv("PrivateKey")).address;
		public static readonly Nethereum.RPC.Eth.DTOs.BlockParameter latestBlock = Nethereum.RPC.Eth.DTOs.BlockParameter.CreateLatest();
	}
	public sealed class BlockchainManager{
		public readonly string node;
		public readonly ulong chainid;
		public static readonly BlockchainManager MintME = new BlockchainManager("https://node1.mintme.com", 24734);

		public static readonly BlockchainManager Polygon = new BlockchainManager("https://polygon-rpc.com", 137);
		public static readonly BlockchainManager BinanceSmartChain = new BlockchainManager("https://bscrpc.com", 56);
		private readonly RpcClient rpc;
		public readonly string tail1;
		private BlockchainManager(string node, ulong chainid)
		{
			this.node = node ?? throw new ArgumentNullException(nameof(node));
			this.chainid = chainid;
			rpc = new RpcClient(new Uri(node));
			tail1 = "\" AND Blockchain = " + chainid + " FOR UPDATE;";
		}
		//Wallet manager pooling
		private readonly ConcurrentDictionary<string, WalletManager> pool = new ConcurrentDictionary<string, WalletManager>();
		public WalletManager GetWalletManager(string privateKey = "0xa85d57fd36432b0e6022d333f3e81b31c67e6afcdb0fa11caf106ff8c29952a9")
		{
			SHA256 sha256 = SHA256.Create();
			string hash = Convert.ToBase64String(sha256.ComputeHash(Encoding.ASCII.GetBytes(privateKey)));
			sha256.Dispose();
			return pool.GetOrAdd(hash, (string disposed) =>
			{
				Account account = new Account(privateKey, chainid);
				return new WalletManager(this, new Web3(account, node).Eth, string.Intern(account.Address.ToLower()));
			});
		}

		public T SendRequestSync<T>(RpcRequest rpcRequest){
			return StaticUtils.Await2(rpc.SendRequestAsync<T>(rpcRequest));
		}
		public void SendRequestSync(RpcRequest rpcRequest)
		{
			StaticUtils.Await2(rpc.SendRequestAsync(rpcRequest));
		}
	}

	public sealed class WalletManager{
		public readonly BlockchainManager blockchainManager;
		private readonly IEthApiContractService ethApiContractService;
		public readonly string address;
		public readonly string trimmedAddress;
		private readonly string tail1;
		private readonly string tail2;
		private readonly IEtherTransferService etherTransferService;

		/// <summary>
		/// DO NOT USE!
		/// </summary>
		public WalletManager(BlockchainManager blockchainManager, IEthApiContractService ethApiContractService, string address)
		{
			this.blockchainManager = blockchainManager ?? throw new ArgumentNullException(nameof(blockchainManager));
			this.ethApiContractService = ethApiContractService ?? throw new ArgumentNullException(nameof(ethApiContractService));
			this.address = address ?? throw new ArgumentNullException(nameof(address));
			trimmedAddress = string.Intern(address.Substring(2));
			etherTransferService = ethApiContractService.GetEtherTransferService();
			tail1 = ", " + blockchainManager.chainid + ", \"" + trimmedAddress + "\");";
			tail2 = " WHERE Blockchain = " + blockchainManager.chainid + " AND Address = \"" + trimmedAddress + "\";";
		}

		public ulong SafeNonce(SQLCommandFactory sqlCommandFactory){
			MySqlDataReader reader = sqlCommandFactory.SafeExecuteReader(sqlCommandFactory.GetCommand("SELECT ExpectedValue FROM Nonces WHERE Address = \"" + address.Substring(2) + blockchainManager.tail1));
			ulong nonce = Convert.ToUInt64(StaticUtils.GetSafeUint(blockchainManager.SendRequestSync<string>(ethApiContractService.Transactions.GetTransactionCount.BuildRequest(address, StaticUtils.latestBlock))).ToString());

			ulong xnonce;
			string query;
			if(reader.HasRows){
				xnonce = reader.GetUInt64("ExpectedValue") + 1;
				StaticUtils.CheckSafety2(nonce > xnonce, "Exchange wallet compromised!");
				reader.CheckSingletonResult();
				query = "UPDATE Nonces SET ExpectedValue = " + xnonce + tail2;
			} else{
				query = "INSERT INTO Nonces (ExpectedValue, Blockchain, Address) VALUES (" + nonce + tail1;
				xnonce = nonce;
			}
			sqlCommandFactory.SafeDestroyReader();
			sqlCommandFactory.SafeExecuteNonQuery(query);
			return xnonce;
		}

		public SafeUint GetEthBalance(){
			string temp = blockchainManager.SendRequestSync<string>(ethApiContractService.GetBalance.BuildRequest(address, StaticUtils.latestBlock));
			return StaticUtils.GetSafeUint(temp);
		}

		public SafeUint GetGasPrice()
		{
			ManualResetEventSlim manualResetEventSlim = new ManualResetEventSlim();
			Task<HexBigInteger> task = ethApiContractService.GasPrice.SendRequestAsync();
			task.GetAwaiter().OnCompleted(manualResetEventSlim.Set);
			manualResetEventSlim.Wait();
			StaticUtils.CheckSafety(task.IsCompleted, "Blockchain request not completed (should not reach here)!");
			Exception e = task.Exception;
			if (e == null)
			{
				return StaticUtils.GetSafeUint(task.Result.ToString());
			}
			else
			{
				throw new SafetyException("Blockchain request failed!", e);
			}
		}

		public string SendEther(SafeUint amount, string to, ulong nonce, SafeUint gasPrice, SafeUint gas)
		{
			Console.WriteLine(gasPrice.GetAmount2().ToString());
			string ret = StaticUtils.Await2(etherTransferService.TransferEtherAsync(to, amount.GetAmount2(), gasPrice.Mul(StaticUtils.gwei).GetAmount2(), gas.bigInteger, new BigInteger(nonce)));
			StaticUtils.CheckSafety(ret, "Null transaction id!");
			return ret;
		}
		public Dictionary<string, object> GetTransactionReceipt(string txid)
		{
			WebRequest httpWebRequest = WebRequest.Create(blockchainManager.node);
			httpWebRequest.Method = "POST";
			httpWebRequest.ContentType = "application/x-www-form-urlencoded";
			byte[] bytes = HttpUtility.UrlEncodeToBytes("\"jsonrpc\":\"2.0\",\"method\":\"eth_getTransactionReceipt\",\"params\":[\"" + txid + "\"],\"id\":1}\"");

			using (var stream = httpWebRequest.GetRequestStream())
			{
				stream.Write(bytes, 0, bytes.Length);
			}
			WebResponse webResponse = httpWebRequest.GetResponse();
			string returns = new StreamReader(webResponse.GetResponseStream()).ReadToEnd();
			webResponse.Close();
			return JsonConvert.DeserializeObject<RpcResult1>(returns).result;
			
		}

		private sealed class RpcResult1{
			private object id;
			private object jsonrpc;
			public Dictionary<string, object> result;
		}
	}

	public sealed class TransactionReceipt{
		public object blockNumber = null;
		public object transactionHash = null;
		public object status = null;
	}
}

