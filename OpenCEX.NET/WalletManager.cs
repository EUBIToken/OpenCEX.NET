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
using Nethereum.Util;
using System.Data;

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
		public static readonly BlockchainManager MintME = new BlockchainManager("https://mintme.polyeubitoken.com/", 24734);

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
			ExchangeWalletManager = GetWalletManager(ReducedInitSelector.set ? "0x5dffe214f70dfe96b1060c5b280721562738588b6ebde8012208c93a576625cd" : Environment.GetEnvironmentVariable("OpenCEX_PrivateKey"));
		}


		public WalletManager GetWalletManager(string privateKey)
		{
			Account account = new Account(privateKey, chainid);
			return new WalletManager(this, new Web3(account, node).Eth, string.Intern(account.Address.ToLower()), privateKey);
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

		public ConcurrentJob GetUpdate(){
			return new UpdateChainInfo(this);
		}

		private readonly string prk;

		/// <summary>
		/// DO NOT USE!
		/// </summary>
		public WalletManager(BlockchainManager blockchainManager, IEthApiContractService ethApiContractService, string address, string prk)
		{
			this.blockchainManager = blockchainManager ?? throw new ArgumentNullException(nameof(blockchainManager));
			this.ethApiContractService = ethApiContractService ?? throw new ArgumentNullException(nameof(ethApiContractService));
			this.address = address ?? throw new ArgumentNullException(nameof(address));
			trimmedAddress = string.Intern(address[2..]);
			tail1 = ", " + blockchainManager.chainid + ", \"" + trimmedAddress + "\");";
			tail2 = " WHERE Blockchain = " + blockchainManager.chainid + " AND Address = \"" + trimmedAddress + "\";";
			if(prk.StartsWith("0x")){
				this.prk = prk[2..];
			} else{
				this.prk = prk;
			}
		}

		public ulong SafeNonce(SQLCommandFactory sqlCommandFactory){
			MySqlDataReader reader = sqlCommandFactory.SafeExecuteReader(sqlCommandFactory.GetCommand("SELECT ExpectedValue FROM Nonces WHERE Address = \"" + address[2..] + blockchainManager.tail1));
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
			StaticUtils.CheckSafety(task.IsCompleted, "Blockchain request not completed (should not reach here)!", true);
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

		public string SignEther(SafeUint amount, string to, ulong nonce, SafeUint gasPrice, SafeUint gas, string data = "")
		{
			TransactionInput transactionInput = new TransactionInput(data, to, address, gas.bigInteger.ToHexBigInteger(), gasPrice.bigInteger.ToHexBigInteger(), amount.bigInteger.ToHexBigInteger())
			{
				Nonce = nonce.ToHexBigInteger()
			};

			string ret = StaticUtils.Await2(ethApiContractService.TransactionManager.SignTransactionAsync(transactionInput));
			StaticUtils.CheckSafety(ret, "Null transaction!");
			return ret;
		}

		public void SendRawTX(string tx){
			RpcRequest rpcRequest = ethApiContractService.Transactions.SendRawTransaction.BuildRequest(tx);
			StaticUtils.Await2(blockchainManager.rpc.SendRequestAsync<string>(rpcRequest));
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
			return blockchainManager.SendRequestSync<TransactionReceipt>(ethApiContractService.Transactions.GetTransactionReceipt.BuildRequest(txid));
		}

		//SafeSend: mitigation for freezer vulnerability
		public void Unsafe_SafeSendEther(SQLCommandFactory sql, SafeUint amount, string to, SafeUint gasPrice, SafeUint gas, string data, ulong userid, bool deposit, string token, SafeUint failureRebate, string failureRebateToken)
		{
			MySqlCommand cmd = sql.GetCommand("INSERT INTO PendingTransactions (DataOrAmount, SendEther, Blockchain, Gas, GasPrice, UserID, CreditTokenSuccess, CreditAmountSuccess, CreditTokenFailure, CreditAmountFailure, Deposit, Dest, FromPrivateKey) VALUES (@a, @b, @c, @d, @e, @f, @g, @h, @i, @j, @k, @l, @m);");

			if (data is null)
			{
				data = amount.ToString();
				cmd.Parameters.AddWithValue("@a", data);
				cmd.Parameters.AddWithValue("@b", true);
			}
			else
			{
				cmd.Parameters.AddWithValue("@a", data);
				cmd.Parameters.AddWithValue("@b", false);
				data = amount.ToString();
			}
			cmd.Parameters.AddWithValue("@c", blockchainManager.chainid);
			cmd.Parameters.AddWithValue("@d", gas.ToString());
			cmd.Parameters.AddWithValue("@e", gasPrice.ToString());
			cmd.Parameters.AddWithValue("@f", userid);
			if(deposit){
				cmd.Parameters.AddWithValue("@g", token);
				cmd.Parameters.AddWithValue("@h", data);
			} else{
				cmd.Parameters.AddWithValue("@g", "scamcoin");
				cmd.Parameters.AddWithValue("@h", "0");
			}
			cmd.Parameters.AddWithValue("@i", failureRebateToken);
			cmd.Parameters.AddWithValue("@j", failureRebate.ToString());
			cmd.Parameters.AddWithValue("@k", deposit);
			cmd.Parameters.AddWithValue("@l", to);
			cmd.Parameters.AddWithValue("@m", prk);
			cmd.Prepare();
			cmd.SafeExecuteNonQuery();
		}

		public ulong GetNonce(){
			return Convert.ToUInt64(StaticUtils.GetSafeUint(blockchainManager.SendRequestSync<string>(ethApiContractService.Transactions.GetTransactionCount.BuildRequest(address, StaticUtils.latestBlock))).ToString());
		}
	}

	public sealed class TransactionReceipt{
		public object blockNumber = null;
		public object transactionHash = null;
		public object status = null;
	}

	public static partial class StaticUtils{
		private sealed class SendingManagerThread{
			public static void Start(){
				Thread thr = new Thread(new SendingManagerThread().DoStupidThings)
				{
					Name = "OpenCEX.NET transaction sending manager thread"
				};
				thr.Start();
				ManagedAbortThread.Append(thr);
			}
			private readonly SQLCommandFactory sql;
			private readonly MySqlCommand read;
			private readonly MySqlCommand delete;
			private readonly MySqlCommand getNonce;
			private readonly MySqlCommand putNonce1;
			private readonly MySqlCommand putNonce2;
			private readonly MySqlCommand appendDeposit;
			private readonly MySqlCommand blacklist;
			private SendingManagerThread(){
				sql = GetSQL(IsolationLevel.RepeatableRead);

				read = sql.GetCommand("SELECT * FROM PendingTransactions ORDER BY Id FOR UPDATE;");

				delete = sql.GetCommand("DELETE FROM PendingTransactions WHERE Id = @id;");
				delete.Parameters.AddWithValue("@id", 0UL);
				delete.Prepare();

				getNonce = sql.GetCommand("SELECT ExpectedValue FROM Nonces WHERE Address = @a AND Blockchain = @b FOR UPDATE;");
				getNonce.Parameters.AddWithValue("@a", string.Empty);
				getNonce.Parameters.AddWithValue("@b", 0UL);
				getNonce.Prepare();

				putNonce1 = sql.GetCommand("INSERT INTO Nonces (Address, Blockchain, ExpectedValue) VALUES (@a, @b, @c);");
				putNonce1.Parameters.AddWithValue("@a", string.Empty);
				putNonce1.Parameters.AddWithValue("@b", 0UL);
				putNonce1.Parameters.AddWithValue("@c", 0UL);
				putNonce1.Prepare();

				putNonce2 = sql.GetCommand("UPDATE Nonces SET ExpectedValue = @c WHERE Address = @a AND Blockchain = @b;");
				putNonce2.Parameters.AddWithValue("@a", string.Empty);
				putNonce2.Parameters.AddWithValue("@b", 0UL);
				putNonce2.Parameters.AddWithValue("@c", 0UL);
				putNonce2.Prepare();

				appendDeposit = sql.GetCommand("INSERT INTO WorkerTasks (LastTouched, URL, URL2) VALUES (@a, @b, @c);");
				appendDeposit.Parameters.AddWithValue("@a", 0UL);
				appendDeposit.Parameters.AddWithValue("@b", string.Empty);
				appendDeposit.Parameters.AddWithValue("@c", string.Empty);
				appendDeposit.Prepare();

				blacklist = sql.GetCommand("UPDATE PendingTransactions SET AlreadySent = 1 WHERE Id = @id;");
				blacklist.Parameters.AddWithValue("@id", 0UL);
				blacklist.Prepare();
			}

			private sealed class TaskDescriptor{
				public readonly string data;
				public readonly SafeUint amt;
				public readonly SafeUint gas;
				public readonly SafeUint gasPrice;
				public readonly bool deposit;
				public readonly SafeUint compensationAmount;
				public readonly string compensationToken;
				public readonly string token;
				public readonly string dest;
				public readonly ulong userid;
				public readonly WalletManager walletManager;
				public readonly ulong id;
				public readonly SafeUint cred2;
				public readonly bool blacklisted;

				public TaskDescriptor(MySqlDataReader mySqlDataReader, IDictionary<string, WalletManager> pool)
				{
					if (mySqlDataReader.GetBoolean("SendEther"))
					{
						data = string.Empty;
						amt = GetSafeUint(mySqlDataReader.GetString("DataOrAmount"));
					}
					else
					{
						data = mySqlDataReader.GetString("DataOrAmount");
						amt = zero;
					}
					BlockchainManager blockchainManager;
					switch (mySqlDataReader.GetUInt64("Blockchain"))
					{
						case 24734:
							blockchainManager = BlockchainManager.MintME;
							break;
						case 137:
							blockchainManager = BlockchainManager.Polygon;
							break;
						case 56:
							blockchainManager = BlockchainManager.BinanceSmartChain;
							break;
						default:
							ThrowInternal2("Invalid blockchain (should not reach here)!");
							return;
					}
					gas = GetSafeUint(mySqlDataReader.GetString("Gas"));
					cred2 = GetSafeUint(mySqlDataReader.GetString("CreditAmountSuccess"));
					gasPrice = GetSafeUint(mySqlDataReader.GetString("GasPrice"));
					deposit = mySqlDataReader.GetBoolean("Deposit");

					compensationAmount = GetSafeUint(mySqlDataReader.GetString("CreditAmountFailure"));
					compensationToken = mySqlDataReader.GetString("CreditTokenFailure");
					token = mySqlDataReader.GetString("CreditTokenSuccess");
					dest = mySqlDataReader.GetString("Dest");
					userid = mySqlDataReader.GetUInt64("UserID");
					id = mySqlDataReader.GetUInt64("Id");
					{
						string privateKey = mySqlDataReader.GetString("FromPrivateKey");
						string selector = blockchainManager.chainid + '_' + privateKey;
						if (!pool.TryGetValue(selector, out WalletManager walletManager1))
						{
							walletManager1 = blockchainManager.GetWalletManager(privateKey);
							CheckSafety(pool.TryAdd(selector, walletManager1), "Unable to pool wallet manager!");
						}
						walletManager = walletManager1;
					}
					blacklisted = mySqlDataReader.GetBoolean("AlreadySent");
				}
			}

			public void DoStupidThings(){
				bool deposited = false;
				while (true)
				{
					byte lim = 0;
					try
					{
						MySqlDataReader mySqlDataReader = read.ExecuteReader();
						Dictionary<string, WalletManager> pool = new Dictionary<string, WalletManager>();
						Queue<TaskDescriptor> taskDescriptors = new Queue<TaskDescriptor>();
						while (mySqlDataReader.Read() && ++lim != 255)
						{
							taskDescriptors.Enqueue(new TaskDescriptor(mySqlDataReader, pool));
						}

						mySqlDataReader.Close();
						while (taskDescriptors.TryDequeue(out TaskDescriptor res) && !abort)
						{
							getNonce.Parameters["@a"].Value = res.walletManager.address;
							getNonce.Parameters["@b"].Value = res.walletManager.blockchainManager.chainid;
							mySqlDataReader = getNonce.ExecuteReader();
							ulong expected;
							MySqlCommand updateNonce;
							if (mySqlDataReader.Read())
							{
								expected = mySqlDataReader.GetUInt64("ExpectedValue") + 1;
								updateNonce = putNonce2;
								CheckSafety2(res.walletManager.GetNonce() > expected, "Exchange wallet compromised!");
							}
							else
							{
								expected = res.walletManager.GetNonce();
								updateNonce = putNonce1;
							}
							mySqlDataReader.CheckSingletonResult();
							mySqlDataReader.Close();

							string tx = res.walletManager.SignEther(res.amt, res.dest, expected, res.gasPrice, res.gas, res.data);
							string txid = TransactionUtils.CalculateTransactionHash(tx);
							if (!res.blacklisted){							
								bool switch2 = false;
								try
								{
									res.walletManager.SendRawTX(tx);
								}
								catch (Exception e)
								{
									Console.Error.WriteLine("Unable to send transaction: " + e);
									txid = null;
								} finally{
									blacklist.Parameters["@id"].Value = res.id;
									
									try{
										blacklist.SafeExecuteNonQuery();
									} catch (Exception e){
										Console.Error.WriteLine("Restarting transaction sending manager due to unexpected exception in battle-shorted sending manager section: " + e);
										switch2 = true;
									}
									if (!switch2)
									{
										switch2 = true;
										try
										{
											sql.DestroyTransaction(true, false);
											switch2 = false;
										}
										catch (Exception e)
										{
											Console.Error.WriteLine("Restarting transaction sending manager due to unexpected exception in battle-shorted sending manager section: " + e);
										}
									}
								}
								if(switch2){
									//The best course of action if we gets here
									//Is to restart transaction sending manager
									Start();
									return;
								} else{
									try{
										sql.BeginTransaction();
									} catch (Exception e){
										Console.Error.WriteLine("Restarting transaction sending manager due to unexpected exception while re-opening MySQL transaction: " + e);
										Start();
										return;
									}
								}
							}
							tx = null;

							if (txid is null)
							{
								sql.Credit(res.compensationToken, res.userid, res.compensationAmount, res.compensationToken == "WMintME");
							}
							else
							{
								updateNonce.Parameters["@a"].Value = res.walletManager.address;
								updateNonce.Parameters["@b"].Value = res.walletManager.blockchainManager.chainid;
								updateNonce.Parameters["@c"].Value = expected;
								updateNonce.SafeExecuteNonQuery();
								if (res.deposit)
								{
									appendDeposit.Parameters["@a"].Value = res.userid;
									appendDeposit.Parameters["@b"].Value = res.token;
									appendDeposit.Parameters["@c"].Value = txid + "_" + res.cred2.ToString();
									appendDeposit.SafeExecuteNonQuery();
									deposited = true;
								}
							}
							delete.Parameters["@id"].Value = res.id;
							delete.SafeExecuteNonQuery();
						}
						try
						{
							sql.DestroyTransaction(true, false);
						}
						catch (Exception e)
						{
							Console.Error.WriteLine("Restarting transaction sending manager due to unexpected exception while destroying MySQL transaction: " + e);
							//The best course of action if we gets here
							//Is to restart transaction sending manager
							Start();
							return;
						}
					}
					catch (Exception e)
					{
						Console.Error.WriteLine("Exception in transaction sending manager: " + e.ToString());
						try
						{
							sql.DestroyTransaction(true, false);
						}
						catch (Exception x)
						{
							Console.Error.WriteLine("Restarting transaction sending manager due to unexpected exception while destroying MySQL transaction: " + x);
							//The best course of action if we gets here
							//Is to restart transaction sending manager
							Start();
							return;
						}
					}
					

					if (deposited && !Multiserver)
					{
						depositBlocker.Set();
						deposited = false;
					}
					if(lim == 0){
						Thread.Sleep(1237);
					}
					if(abort){
						break;
					} else{
						try{
							sql.BeginTransaction();
						} catch (Exception e){
							Console.Error.WriteLine("Restarting transaction sending manager due to unexpected exception while opening MySQL transaction: " + e);
							//The best course of action if we gets here
							//Is to restart transaction sending manager
							Start();
							return;
						}
					}
				}
				Console.WriteLine(Thread.CurrentThread.Name + " stopped!");
			}
		}
	}
}

