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
using Nethereum.Contracts.ContractHandlers;
using Nethereum.RPC.Eth.DTOs;

namespace jessielesbian.OpenCEX
{
	public static partial class StaticUtils
	{
		public static readonly string ExchangeWalletAddress = BlockchainManager.MintME.ExchangeWalletManager.address;
		public static readonly BlockParameter latestBlock = BlockParameter.CreateLatest();
	}
	public sealed class BlockchainManager{
		public readonly string node;
		public readonly ulong chainid;
		public static readonly BlockchainManager MintME = new BlockchainManager("https://node1.mintme.com", 24734);

		public static readonly BlockchainManager Polygon = new BlockchainManager("https://polygon-rpc.com", 137);
		public static readonly BlockchainManager BinanceSmartChain = new BlockchainManager("https://bscrpc.com", 56);
		public readonly RpcClient rpc;
		public readonly string tail1;
		public readonly WalletManager ExchangeWalletManager;
		private BlockchainManager(string node, ulong chainid)
		{
			this.node = node ?? throw new ArgumentNullException(nameof(node));
			this.chainid = chainid;
			rpc = new RpcClient(new Uri(node));
			tail1 = "\" AND Blockchain = " + chainid + " FOR UPDATE;";
			ExchangeWalletManager = GetWalletManager(Environment.GetEnvironmentVariable("OpenCEX_PrivateKey"));
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
		public ulong SafeBlockheight { get; private set; } = 0;
		private sealed class UpdateChainInfo : ConcurrentJob
		{
			private readonly WalletManager parent;

			public UpdateChainInfo(WalletManager parent)
			{
				this.parent = parent ?? throw new ArgumentNullException(nameof(parent));
			}

			protected override object ExecuteIMPL()
			{
				parent.SafeBlockheight = Convert.ToUInt64(StaticUtils.Await2(parent.ethApiContractService.Blocks.GetBlockNumber.SendRequestAsync()).ToString()) + 10;
				return null;
			}
		}

		public ConcurrentJob getUpdate(){
			return new UpdateChainInfo(this);
		}

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

		public string SendEther(SafeUint amount, string to, ulong nonce, SafeUint gasPrice, SafeUint gas, string data = "")
		{
			TransactionInput transactionInput = new TransactionInput(data, to, address, gas.bigInteger.ToHexBigInteger(), gasPrice.bigInteger.ToHexBigInteger(), amount.bigInteger.ToHexBigInteger());
			transactionInput.Nonce = nonce.ToHexBigInteger();

			string ret = StaticUtils.Await2(ethApiContractService.TransactionManager.SendTransactionAsync(transactionInput));
			StaticUtils.CheckSafety(ret, "Null transaction id!");
			return ret;
		}

		[JsonObject(MemberSerialization.Fields)]
		private sealed class EthCall{
			public readonly string from;
			public readonly string to;
			public readonly HexBigInteger gasprice;
			public readonly HexBigInteger value;
			public readonly string data;

			public EthCall(string from, string to, HexBigInteger gasprice, HexBigInteger value, string data)
			{
				this.from = from ?? throw new ArgumentNullException(nameof(from));
				this.to = to ?? throw new ArgumentNullException(nameof(to));
				this.gasprice = gasprice ?? throw new ArgumentNullException(nameof(gasprice));
				this.value = value ?? throw new ArgumentNullException(nameof(value));
				this.data = data ?? throw new ArgumentNullException(nameof(data));
			}
		}
		private string Vcall2(string to, SafeUint gasprice, SafeUint value, string data, string method)
		{
			RpcRequest rpcRequest = new RpcRequest(1, method, new EthCall(address, to, gasprice.bigInteger.ToHexBigInteger(), value.bigInteger.ToHexBigInteger(), data), "latest");
			return blockchainManager.SendRequestSync<string>(rpcRequest);
		}
		public string Vcall(string to, SafeUint gasprice, SafeUint value, string data)
		{
			return Vcall2(to, gasprice, value, data, "eth_call");
		}
		public SafeUint EstimateGas(string to, SafeUint gasprice, SafeUint value, string data)
		{
			return StaticUtils.GetSafeUint(Vcall2(to, gasprice, value, data, "eth_estimateGas"));
		}
		public TransactionReceipt GetTransactionReceipt(string txid)
		{
			Console.WriteLine(JsonConvert.SerializeObject(blockchainManager.SendRequestSync<JValue>(ethApiContractService.Transactions.GetTransactionReceipt.BuildRequest(txid))));
			return blockchainManager.SendRequestSync<TransactionReceipt>(ethApiContractService.Transactions.GetTransactionReceipt.BuildRequest(txid));
		}

		private sealed class RpcResult1{
			public object id;
			public object jsonrpc;
			public Dictionary<string, object> result;
		}
	}

	public sealed class TransactionReceipt{
		public object blockNumber = null;
		public object transactionHash = null;
		public object status = null;
	}
}

