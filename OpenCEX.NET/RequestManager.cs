using jessielesbian.OpenCEX;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using jessielesbian.OpenCEX.RequestManager;
using jessielesbian.OpenCEX.SafeMath;
using MySql.Data.MySqlClient;
using System.Text;
using System.Numerics;
using Org.BouncyCastle.Crypto.Generators;
using System.Security.Cryptography;
using Newtonsoft.Json;
using System.IO;
using System.Web;
using Nethereum.Util;
using System.Threading;
using FreeRedis;
using System.Data;

namespace jessielesbian.OpenCEX.RequestManager
{
	public sealed class Request : ConcurrentJob
	{
		public readonly RequestMethod method;
		public readonly HttpListenerContext httpListenerContext;
		public readonly IDictionary<string, object> args;
		public readonly SQLCommandFactory sqlCommandFactory;
		public readonly SharedAuthenticationHint sharedAuthenticationHint;
		

		public Request(SQLCommandFactory sqlCommandFactory, RequestMethod method, HttpListenerContext httpListenerContext, IDictionary<string, object> args, SharedAuthenticationHint sharedAuthenticationHint)
		{
			if(method.needSQL){
				this.sqlCommandFactory = sqlCommandFactory ?? throw new ArgumentNullException(nameof(sqlCommandFactory));
			} else{
				this.sqlCommandFactory = null;
			}
			
			this.method = method ?? throw new ArgumentNullException(nameof(method));
			this.httpListenerContext = httpListenerContext ?? throw new ArgumentNullException(nameof(httpListenerContext));
			this.args = args ?? throw new ArgumentNullException(nameof(args));
			this.sharedAuthenticationHint = sharedAuthenticationHint ?? throw new ArgumentNullException(nameof(sharedAuthenticationHint));
			
		}

		public ulong GetUserID()
		{
			lock(sharedAuthenticationHint){
				if (sharedAuthenticationHint.touched)
				{
					StaticUtils.CheckSafety2(sharedAuthenticationHint.userid == 0, "Previous authentication attempt failed!");
				} else{
					sharedAuthenticationHint.touched = true;
					Cookie cookie;
					lock (httpListenerContext)
					{
						cookie = httpListenerContext.Request.Cookies["__Secure-OpenCEX_session"];
					}
					StaticUtils.CheckSafety(cookie, "Missing session token!");

					byte[] bytes;
					try{
						bytes = Convert.FromBase64String(WebUtility.UrlDecode(cookie.Value));
					} catch{
						throw new SafetyException("Invalid session token!");
					}

					StaticUtils.CheckSafety(bytes.Length == 64, "Invalid session token!");

					SHA256 hash = SHA256.Create();
					string result = "SESSION_" + BitConverter.ToString(hash.ComputeHash(bytes)).Replace("-", string.Empty);
					hash.Dispose();

					lock (sharedAuthenticationHint.redisClient)
					{
						string struserid = sharedAuthenticationHint.redisClient.Get(result);
						StaticUtils.CheckSafety(struserid, "Invalid session token!");
						sharedAuthenticationHint.userid = Convert.ToUInt64(struserid);
					}
				}
				return sharedAuthenticationHint.userid;
			}
			
		}

		protected override object ExecuteIMPL()
		{
			object ret;
			try
			{
				ret = method.Execute(this);
				if(method.needSQL){
					sqlCommandFactory.DestroyTransaction(true, true);
				}
			} catch(Exception e){
				try{
					if (method.needSQL)
					{
						sqlCommandFactory.DestroyTransaction(false, true);
					}
				} catch{
					
				}
				if(e is ISafetyException)
				{
					throw;
				} else{
					throw new SafetyException("Unexpected internal server error!", e);
				}
				
			}
			return ret;
			

		}
	}

	public abstract class RequestMethod{
		public abstract object Execute(Request request);
		protected abstract bool NeedRedis();
		protected abstract IsolationLevel SQLMode();
		public readonly bool needSQL;
		public readonly bool needRedis;
		public readonly IsolationLevel SQLMode2;

		public RequestMethod(){
			needRedis = NeedRedis();
			SQLMode2 = SQLMode();
			needSQL = SQLMode2 != IsolationLevel.Unspecified;
		}
	}
}

namespace jessielesbian.OpenCEX{
	public static partial class StaticUtils
	{
		private sealed class TestShitcoins : RequestMethod
		{
			private TestShitcoins()
			{

			}

			public static readonly RequestMethod instance = new TestShitcoins();

			public override object Execute(Request request)
			{
				//NOTE: Shortfall protection is disabled, since we are depositing.
				ulong userId = request.GetUserID();
				request.Credit("shitcoin", userId, ether, false);
				request.Credit("scamcoin", userId, ether, false);
				return null;
			}

			protected override bool NeedRedis()
			{
				return true;
			}

			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.RepeatableRead;
			}
		}

		private sealed class CancelOrder : RequestMethod
		{
			private CancelOrder()
			{

			}

			public static readonly RequestMethod instance = new CancelOrder();

			public override object Execute(Request request)
			{
				CheckSafety(request.args.TryGetValue("target", out object target2), "Order cancellation must specify target!");
				ulong target;
				try
				{
					target = Convert.ToUInt64(target2);
				}
				catch
				{
					throw new SafetyException("Target must be unsigned number!");
				}

				ulong userid = request.GetUserID();

				MySqlDataReader mySqlDataReader = request.sqlCommandFactory.SafeExecuteReader(request.sqlCommandFactory.GetCommand("SELECT PlacedBy, Pri, Sec, InitialAmount, TotalCost, Buy FROM Orders WHERE Id = " + target + " FOR UPDATE;"));
				CheckSafety(mySqlDataReader.HasRows, "Nonexistant order!");
				CheckSafety(mySqlDataReader.GetUInt64("PlacedBy") == userid, "Attempted to cancel another user's order!");
				string refund;
				if (mySqlDataReader.GetUInt32("Buy") == 0)
				{
					refund = mySqlDataReader.GetString("Sec");
				}
				else
				{
					refund = mySqlDataReader.GetString("Pri");
				}
				SafeUint amount = GetSafeUint(mySqlDataReader.GetString("InitialAmount")).Sub(GetSafeUint(mySqlDataReader.GetString("TotalCost")));

				mySqlDataReader.CheckSingletonResult();
				request.sqlCommandFactory.SafeDestroyReader();

				request.sqlCommandFactory.SafeExecuteNonQuery("DELETE FROM Orders WHERE Id = " + target + ";");
				request.Credit(refund, userid, amount, true);
				return null;
			}

			protected override bool NeedRedis()
			{
				return true;
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.RepeatableRead;
			}
		}

		private sealed class PlaceOrder : RequestMethod
		{
			private PlaceOrder()
			{

			}

			public static readonly RequestMethod instance = new PlaceOrder();

			public override object Execute(Request request)
			{
				//Safety checks
				long fillMode = request.ExtractRequestArg<long>("fill_mode");
				SafeUint price = request.ExtractSafeUint("price");
				SafeUint amount = request.ExtractSafeUint("amount");
				string primary = request.ExtractRequestArg<string>("primary");
				string secondary = request.ExtractRequestArg<string>("secondary");
				bool buy = request.ExtractRequestArg<bool>("buy");
				if (buy)
				{
					CheckSafety2(price.isZero, "Zero-price buy order!");
				}

				CheckSafety(fillMode > -1, "Invalid fill mode!");
				CheckSafety(fillMode < 3, "Invalid fill mode!");
				try
				{
					GetEnv("PairExists_" + primary.Replace("_", "__") + "_" + secondary);
				}
				catch
				{
					throw new SafetyException("Nonexistant trading pair!");
				}

				string selected;
				string output;
				bool sell;
				SafeUint amt2;
				MySqlCommand counter;
				if (buy)
				{
					sell = false;
					selected = primary;
					output = secondary;
					amt2 = amount.Mul(ether).Div(price);
					counter = request.sqlCommandFactory.GetCommand("SELECT Price, Amount, InitialAmount, TotalCost, Id, PlacedBy FROM Orders WHERE Pri = @primary AND Sec = @secondary AND Buy = 0 ORDER BY Price DESC, Id ASC FOR UPDATE;");
				}
				else
				{
					sell = true;
					selected = secondary;
					output = primary;
					amt2 = amount;
					counter = request.sqlCommandFactory.GetCommand("SELECT Price, Amount, InitialAmount, TotalCost, Id, PlacedBy FROM Orders WHERE Pri = @primary AND Sec = @secondary AND Buy = 1 ORDER BY Price ASC, Id ASC FOR UPDATE;");
				}
				counter.Parameters.AddWithValue("@primary", primary);
				counter.Parameters.AddWithValue("@secondary", secondary);
				counter.Prepare();

				if (fillMode == 0)
				{
					CheckSafety2(amount.isZero, "Zero limit order size!");
					CheckSafety2(amount < GetSafeUint(GetEnv("MinimumLimit_" + selected)), "Order is smaller than minimum limit order size!");
				}

				Queue<Order> moddedOrders = new Queue<Order>();
				SafeUint close = null;

				ulong userid = request.GetUserID();
				request.Debit(selected, userid, amount, true);
				request.Credit(output, userid, zero, true);
				LPReserve lpreserve = new LPReserve(request.sqlCommandFactory, primary, secondary);
				MySqlDataReader reader = request.sqlCommandFactory.SafeExecuteReader(counter);
				Order instance = new Order(price, amt2, amount, zero, userid, 0);
				SafeUint old = lpreserve.reserve0;
				if (reader.HasRows)
				{
					bool read = true;
					while (read)
					{
						Order other = new Order(GetSafeUint(reader.GetString("Price")), GetSafeUint(reader.GetString("Amount")), GetSafeUint(reader.GetString("InitialAmount")), GetSafeUint(reader.GetString("TotalCost")), reader.GetUInt64("PlacedBy"), reader.GetUInt64("Id"));
						if ((buy && instance.price < other.price) || (sell && instance.price > other.price))
						{
							break;
						}
						if (other.Balance.isZero || other.amount.isZero)
						{
							read = reader.Read();
							continue;
						}

						lpreserve = TryArb(request.sqlCommandFactory, primary, secondary, buy, instance, other.price, lpreserve);
						if (old != lpreserve.reserve0)
						{
							close = lpreserve.reserve0.Mul(ether).Div(lpreserve.reserve1);
						}
						SafeUint oldamt1 = instance.Balance;
						SafeUint oldamt2 = other.Balance;
						if (oldamt1.isZero || instance.amount.isZero)
						{
							break;
						}
						else if (MatchOrders(instance, other, buy))
						{
							moddedOrders.Enqueue(other);
							close = other.price;
							SafeUint outamt = oldamt1.Sub(instance.Balance);
							request.Credit(output, userid, oldamt2.Sub(other.Balance), true);
							request.Credit(selected, other.placedby, outamt, true);
							read = reader.Read();
							if (!other.Balance.isZero)
							{
								lpreserve = TryArb(request.sqlCommandFactory, primary, secondary, sell, other, other.price, lpreserve);
							}
						}
						else
						{
							break;
						}
					}
				}

				request.sqlCommandFactory.SafeDestroyReader();
				SafeUint balance2 = instance.Balance;
				if (!balance2.isZero)
				{
					//Fill the rest of the order with Uniswap.NET
					old = lpreserve.reserve0;
					lpreserve = TryArb(request.sqlCommandFactory, primary, secondary, buy, instance, instance.price, lpreserve);
					if (old != lpreserve.reserve0)
					{
						close = lpreserve.reserve0.Mul(ether).Div(lpreserve.reserve1);
					}

					//Tail safety check
					SafeUint amount3;
					balance2 = instance.Balance;
					if (buy)
					{
						amount3 = balance2.Mul(ether).Div(price);
					}
					else
					{
						amount3 = balance2;
					}

					//We only save the order to database if it's a limit order and it's not fully executed.
					if (instance.amount.isZero || fillMode == 1)
					{
						//Cancel order
						request.Credit(selected, userid, instance.Balance, true);
						goto admitted;
					}
					else
					{
						CheckSafety2(fillMode == 2, "Fill or kill order canceled due to insufficient liquidity!");
					}
					StringBuilder stringBuilder = new StringBuilder("INSERT INTO Orders (Pri, Sec, Price, Amount, InitialAmount, TotalCost, PlacedBy, Buy) VALUES (@primary, @secondary, \"");
					stringBuilder.Append(instance.price.ToString() + "\", \"");
					stringBuilder.Append(amount3.ToString() + "\", \"");
					stringBuilder.Append(amount.ToString() + "\", \"");
					stringBuilder.Append(instance.totalCost.ToString() + "\", \"");
					stringBuilder.Append(userid.ToString() + (buy ? "\", 1);" : "\", 0);"));
					MySqlCommand mySqlCommand = request.sqlCommandFactory.GetCommand(stringBuilder.ToString());
					mySqlCommand.Parameters.AddWithValue("@primary", primary);
					mySqlCommand.Parameters.AddWithValue("@secondary", secondary);
					mySqlCommand.Prepare();
					mySqlCommand.ExecuteNonQuery();
				}

			admitted:
				WriteLP(request.sqlCommandFactory, primary, secondary, lpreserve);
				MySqlCommand update = request.sqlCommandFactory.GetCommand("UPDATE Orders SET Amount = @amt, TotalCost = @cost WHERE Id = @id;");
				update.Parameters.AddWithValue("@amt", string.Empty);
				update.Parameters.AddWithValue("@cost", string.Empty);
				update.Parameters.AddWithValue("@id", 0UL);
				update.Prepare();
				MySqlCommand delete = request.sqlCommandFactory.GetCommand("DELETE FROM Orders WHERE Id = @id;");
				delete.Parameters.AddWithValue("@id", 0UL);
				delete.Prepare();

				while (moddedOrders.TryDequeue(out Order modded))
				{
					MySqlCommand action;
					if (modded.amount.isZero)
					{
						SafeUint balance = modded.Balance;
						if (!balance.isZero)
						{
							request.Credit(output, modded.placedby, balance, true);
						}

						action = delete;
					}
					else if (modded.Balance.isZero)
					{
						action = delete;
					}
					else
					{
						action = update;
						action.Parameters["@amt"].Value = modded.amount.ToString();
						action.Parameters["@cost"].Value = modded.totalCost.ToString();
					}
					action.Parameters["@id"].Value = modded.id;
					action.SafeExecuteNonQuery();
				}

				if (close != null)
				{
					//Async update chart to avoid blocking request/failure propagation
					request.sqlCommandFactory.AfterCommit(new UpdateChart(primary, secondary, close));
				}

				return null;
			}
			protected override bool NeedRedis()
			{
				return true;
			}

			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.RepeatableRead;
			}
		}

		private sealed class UpdateChart : ConcurrentJob
		{
			private readonly string primary;
			private readonly string secondary;
			private readonly SafeUint update;

			public UpdateChart(string primary, string secondary, SafeUint update)
			{
				this.primary = primary ?? throw new ArgumentNullException(nameof(primary));
				this.secondary = secondary ?? throw new ArgumentNullException(nameof(secondary));
				this.update = update ?? throw new ArgumentNullException(nameof(update));
			}

			protected override object ExecuteIMPL()
			{
				SQLCommandFactory sqlCommandFactory = GetSQL(IsolationLevel.RepeatableRead);
				bool commit;
				try
				{
					MySqlCommand prepared = sqlCommandFactory.GetCommand("SELECT Timestamp, Open, High, Low, Close FROM HistoricalPrices WHERE Pri = @primary AND Sec = @secondary ORDER BY Timestamp DESC FOR UPDATE;");
					prepared.Parameters.AddWithValue("@primary", primary);
					prepared.Parameters.AddWithValue("@secondary", secondary);
					prepared.Prepare();
					MySqlDataReader reader = sqlCommandFactory.SafeExecuteReader(prepared);
					SafeUint start = new SafeUint(new BigInteger(DateTimeOffset.Now.ToUnixTimeSeconds()));
					start = start.Sub(start.Mod(day));
					SafeUint high;
					SafeUint low;
					SafeUint open;
					SafeUint time;
					SafeUint close = update;
					bool append;
					if (reader.HasRows)
					{
						time = GetSafeUint(reader.GetString("Timestamp"));
						append = start.Sub(time) > day;
						if (append)
						{
							open = GetSafeUint(reader.GetString("Close"));
							high = open.Max(close);
							low = open.Min(close);
							time = start;
						}
						else
						{
							open = GetSafeUint(reader.GetString("Open"));
							high = GetSafeUint(reader.GetString("High"));
							low = GetSafeUint(reader.GetString("Low"));
						}

					}
					else
					{
						open = zero;
						low = zero;
						high = close;
						append = true;
						time = start;
					}

					sqlCommandFactory.SafeDestroyReader();

					if (append)
					{
						prepared = sqlCommandFactory.GetCommand("INSERT INTO HistoricalPrices (Open, High, Low, Close, Timestamp, Pri, Sec) VALUES (@open, @high, @low, @close, @timestamp, @primary, @secondary);");
					}
					else
					{
						prepared = sqlCommandFactory.GetCommand("UPDATE HistoricalPrices SET Open = @open, High = @high, Low = @low, Close = @close WHERE Timestamp = @timestamp AND Pri = @primary AND Sec = @secondary;");
					}

					if (close > high)
					{
						high = close;
					}

					if (close < low)
					{
						low = close;
					}

					prepared.Parameters.AddWithValue("@open", open.ToString());
					prepared.Parameters.AddWithValue("@high", high.ToString());
					prepared.Parameters.AddWithValue("@low", low.ToString());
					prepared.Parameters.AddWithValue("@close", close.ToString());
					prepared.Parameters.AddWithValue("@timestamp", time.ToString());
					prepared.Parameters.AddWithValue("@primary", primary);
					prepared.Parameters.AddWithValue("@secondary", secondary);
					prepared.Prepare();
					CheckSafety(prepared.ExecuteNonQuery() == 1, "Excessive write effect (should not reach here)!", true);
					commit = true;
				}
				catch (Exception e)
				{
					Console.Error.WriteLine("Unexpected exception while writing chart: " + e.ToString());
					commit = false;
				}
				try
				{
					sqlCommandFactory.DestroyTransaction(commit, true);
				}
				catch (Exception e)
				{
					Console.Error.WriteLine("Unexpected exception while closing chart: " + e.ToString());
				}
				return null;
			}
		}

		public static bool MatchOrders(Order first, Order second, bool buy)
		{
			SafeUint ret = first.amount.Min(second.amount);
			if (buy)
			{
				ret = ret.Min(first.Balance.Mul(ether).Div(second.price)).Min(second.Balance);
				if (second.price > first.price)
				{
					return false;
				}
				else
				{
					first.Debit(ret, second.price);
					second.Debit(ret);
				}
			}
			else
			{
				ret = ret.Min(first.Balance).Min(second.Balance.Mul(ether).Div(second.price));
				if (first.price > second.price)
				{
					return false;
				}
				else
				{
					first.Debit(ret);
					second.Debit(ret, second.price);
				}
			}
			CheckSafety2(ret.isZero, "Order matched without output (should not reach here)!", true);
			return true;
		}

		public class Order
		{
			public readonly SafeUint price;
			public SafeUint amount;
			public readonly SafeUint initialAmount;
			public SafeUint totalCost;
			public readonly ulong placedby;
			public readonly ulong id;

			public Order(SafeUint price, SafeUint amount, SafeUint initialAmount, SafeUint totalCost, ulong placedby, ulong id)
			{
				this.initialAmount = initialAmount ?? throw new ArgumentNullException(nameof(initialAmount));
				this.totalCost = totalCost ?? throw new ArgumentNullException(nameof(totalCost));
				this.price = price ?? throw new ArgumentNullException(nameof(price));
				this.amount = amount ?? throw new ArgumentNullException(nameof(amount));
				this.id = id;
				this.placedby = placedby;
				Balance = initialAmount.Sub(totalCost);
			}
			public void Debit(SafeUint amt, SafeUint price = null)
			{
				SafeUint temp;

				if (price is null)
				{
					temp = totalCost.Add(amt);
				}
				else
				{
					temp = totalCost.Add(amt.Mul(price).Div(ether));
				}
				Balance = initialAmount.Sub(temp, "Negative order size (should not reach here)!", true);
				amount = amount.Sub(amt, "Negative order amount (should not reach here)!", true);
				totalCost = temp;
			}

			public SafeUint Balance;
		}

		//Ported from PHP server
		private static SafeUint GetBidOrAsk(SQLCommandFactory sqlCommandFactory, string pri, string sec, bool bid)
		{
			MySqlCommand mySqlCommand;
			if (bid)
			{
				mySqlCommand = sqlCommandFactory.GetCommand("SELECT Price FROM Orders WHERE Pri = @primary AND Sec = @secondary AND Buy = 1 ORDER BY Price DESC LIMIT 1;");
			}
			else
			{
				mySqlCommand = sqlCommandFactory.GetCommand("SELECT Price FROM Orders WHERE Pri = @primary AND Sec = @secondary AND Buy = 0 ORDER BY Price ASC LIMIT 1;");
			}

			mySqlCommand.Parameters.AddWithValue("@primary", pri);
			mySqlCommand.Parameters.AddWithValue("@secondary", sec);
			mySqlCommand.Prepare();
			MySqlDataReader reader = sqlCommandFactory.SafeExecuteReader(mySqlCommand);
			SafeUint returns;
			if (reader.HasRows)
			{
				returns = GetSafeUint(reader.GetString("Price"));
				reader.CheckSingletonResult();

			}
			else
			{
				returns = null;
			}
			sqlCommandFactory.SafeDestroyReader();
			return returns;
		}

		private sealed class BidAsk : RequestMethod
		{
			public static readonly RequestMethod instance = new BidAsk();
			private BidAsk()
			{

			}
			public override object Execute(Request request)
			{
				string pri = request.ExtractRequestArg<string>("primary");
				string sec = request.ExtractRequestArg<string>("secondary");

				return new string[] { SafeSerializeSafeUint(GetBidOrAsk(request.sqlCommandFactory, pri, sec, true)), SafeSerializeSafeUint(GetBidOrAsk(request.sqlCommandFactory, pri, sec, false)) };
			}

			protected override bool NeedRedis()
			{
				return false;
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.ReadUncommitted;
			}
		}


		private sealed class Deposit : RequestMethod
		{
			public static readonly RequestMethod instance = new Deposit();
			private Deposit()
			{

			}
			public override object Execute(Request request)
			{

				string token;
				{
					request.args.TryGetValue("token", out object temp);
					token = (string)temp;
				}
				BlockchainManager blockchainManager;
				switch (token)
				{
					//Traditional tokens
					case "EUBI":
					case "1000x":
					case "CLICK":
					case "Haoma":
					case "MS-Coin":
						blockchainManager = BlockchainManager.MintME;
						break;
					case "MATIC":
					case "PolyEUBI":
					case "Dai":
						blockchainManager = BlockchainManager.Polygon;
						break;
					case "BNB":
						blockchainManager = BlockchainManager.BinanceSmartChain;
						break;

					//Multichain (MintME-Polygon) token
					case "MintME":
						string blockchain;
						{
							CheckSafety(request.args.TryGetValue("blockchain", out object tm), "Must specify blockchain for multichain token!");
							blockchain = (string)tm;
						}
						blockchainManager = blockchain switch
						{
							"MintME" => BlockchainManager.MintME,
							"Polygon" => BlockchainManager.Polygon,
							_ => throw new SafetyException("Unsupported blockchain!"),
						};
						break;
					//Unknown tokens
					default:
						throw new SafetyException("Unknown token!");
				}
				ulong userid = request.GetUserID();

				MySqlDataReader reader = request.sqlCommandFactory.SafeExecuteReader(request.sqlCommandFactory.GetCommand("SELECT DepositPrivateKey FROM Accounts WHERE UserID = " + userid + ";"));
				WalletManager walletManager = blockchainManager.GetWalletManager(reader.GetString("DepositPrivateKey"));
				reader.CheckSingletonResult();
				request.sqlCommandFactory.SafeDestroyReader();

				string token_address;
				switch (token)
				{
					case "CLICK":
						token_address = "0xf4811b341af177bde2407b976311af66c4b08021";
						break;
					case "Haoma":
						token_address = "0x60ac85dda46937251eb7473a68a1b9367ee2a1f1";
						break;
					case "MS-Coin":
						token_address = "0x34c171a4ee5a3e6ad7ea1b356600e30d7c333d5e";
						break;
					case "1000x":
						token_address = "0x7b535379bbafd9cd12b35d91addabf617df902b2";
						break;
					case "EUBI":
						token_address = "0x8afa1b7a8534d519cb04f4075d3189df8a6738c1";
						break;
					case "PolyEUBI":
						token_address = "0x553e77f7f71616382b1545d4457e2c1ee255fa7a";
						break;
					case "Dai":
						token_address = "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063";
						break;
					case "MintME":
						if (blockchainManager.chainid == 24734)
						{
							token_address = null;
						}
						else
						{
							token_address = "0x2b7bede8a97021da880e6c84e8b915492d2ae216";
							token = "WMintME";
						}
						break;
					default:
						token_address = null;
						break;
				}

				SafeUint gasPrice = walletManager.GetGasPrice();

				//Boost gas price to reduce server waiting time.
				gasPrice = gasPrice.Add(gasPrice.Div(ten));
				SafeUint amount;

				if (token_address is null)
				{
					amount = walletManager.GetEthBalance().Sub(gasPrice.Mul(basegas), "Amount not enough to cover blockchain fee!", false);
					CheckSafety2(amount.isZero, "Zero-value deposit!");
					walletManager.Unsafe_SafeSendEther(request.sqlCommandFactory, amount, ExchangeWalletAddress, gasPrice, basegas, null, userid, true, token, zero, "shitcoin");
				}
				else
				{
					string formattedTokenAddress = ExpandABIAddress(token_address);
					string postfix = formattedTokenAddress + ExpandABIAddress(walletManager.address);
					walletManager = blockchainManager.ExchangeWalletManager;
					string abi2 = "0xaec6ed90" + ExpandABIAddress(walletManager.address) + postfix;

					string ERC20DepositManager;
					string gastoken;
					switch (blockchainManager.chainid)
					{
						case 24734:
							gastoken = "MintME";
							ERC20DepositManager = "0x9f46db28f5d7ef3c5b8f03f19eea5b7aa8621349";
							break;
						case 137:
							gastoken = "MATIC";
							ERC20DepositManager = "0xed91faa6efa532b40f6a1bff3cab29260ebabd21";
							break;
						default:
							throw new SafetyException("Unsupported blockchain!");
					}
					try
					{
						amount = GetSafeUint(walletManager.Vcall(ERC20DepositManager, gasPrice, zero, abi2));
					}
					catch
					{
						amount = GetSafeUint(walletManager.Vcall(ERC20DepositManager, zero, zero, abi2));
					}

					CheckSafety2(amount.isZero, "Zero-value deposit!");
					string abi = "0x64d7cd50" + postfix + amount.ToHex(false);
					SafeUint gas = walletManager.EstimateGas(ERC20DepositManager, gasPrice, zero, abi);
					SafeUint gasFees = gas.Mul(gasPrice);
					request.Debit(gastoken, userid, gasFees, false); //Debit gas token to pay for gas
					walletManager.Unsafe_SafeSendEther(request.sqlCommandFactory, amount, ERC20DepositManager, gasPrice, gas, abi, userid, true, token, gasFees, gastoken);
				}

				return null;
			}

			protected override bool NeedRedis()
			{
				return true;
			}

			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.RepeatableRead;
			}
		}

		private sealed class GetBalances : RequestMethod
		{
			public static readonly RequestMethod instance = new GetBalances();
			private GetBalances()
			{

			}
			public override object Execute(Request request)
			{
				MySqlDataReader mySqlDataReader = request.sqlCommandFactory.GetCommand("SELECT Coin, Balance FROM Balances WHERE UserID = " + request.GetUserID() + " ORDER BY Coin DESC;").ExecuteReader();
				object ret;
				try
				{
					Dictionary<string, string> balances = new Dictionary<string, string>();
					while (mySqlDataReader.Read())
					{
						CheckSafety(balances.TryAdd(mySqlDataReader.GetString("Coin"), mySqlDataReader.GetString("Balance")), "Corrupted balances table (should not reach here)!", true);
					}

					ret = balances;
				}
				catch (Exception x)
				{
					ret = x;
				}
				finally
				{
					mySqlDataReader.Close();
				}

				if (ret is Exception e)
				{
					throw new SafetyException("Unexpected internal server error while fetching user balance!", e);
				}
				else if (ret is Dictionary<string, string> balances)
				{
					List<string[]> returning = new List<string[]>();
					foreach (string token in listedTokensHint)
					{
						if (balances.TryGetValue(token, out string bal))
						{
							returning.Add(new string[] { token, bal });
						}
						else
						{
							returning.Add(new string[] { token, "0" });
						}
					}
					return returning.ToArray();
				}
				else
				{
					ThrowInternal2("Unexpected type while fetching user balance (should not reach here)!");
					return null;
				}

			}

			protected override bool NeedRedis()
			{
				return true;
			}

			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.ReadUncommitted;
			}
		}

		private sealed class GetUsername : RequestMethod
		{
			public static readonly GetUsername instance = new GetUsername();
			private GetUsername()
			{

			}
			public override object Execute(Request request)
			{
				MySqlDataReader mySqlDataReader = request.sqlCommandFactory.GetCommand("SELECT Username FROM Accounts WHERE UserID = " + request.GetUserID() + ";").ExecuteReader();
				object ret;
				try
				{
					CheckSafety(mySqlDataReader.Read(), "Invalid UserID (should not reach here)!", true);
					ret = mySqlDataReader.GetString("Username");
					mySqlDataReader.CheckSingletonResult();
				}
				catch (Exception x)
				{
					ret = x;
				}
				finally
				{
					mySqlDataReader.Close();
				}

				if (ret is Exception e)
				{
					throw new SafetyException("Unexpected internal server error while fetching username (should not reach here)!", e);
				}
				else if (ret is string)
				{
					return ret;
				}
				else
				{
					ThrowInternal2("Unexpected type while fetching user balance (should not reach here)!");
					return null;
				}
			}

			protected override bool NeedRedis()
			{
				return true;
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.ReadUncommitted;
			}
		}

		private sealed class GetEthDepAddr : RequestMethod
		{
			public static readonly GetEthDepAddr instance = new GetEthDepAddr();
			private GetEthDepAddr()
			{

			}
			public override object Execute(Request request)
			{
				MySqlDataReader mySqlDataReader = request.sqlCommandFactory.GetCommand("SELECT DepositPrivateKey FROM Accounts WHERE UserID = " + request.GetUserID() + ";").ExecuteReader();
				object ret;
				try
				{
					CheckSafety(mySqlDataReader.Read(), "Invalid UserID (should not reach here)!", true);
					ret = mySqlDataReader.GetString("DepositPrivateKey");
					mySqlDataReader.CheckSingletonResult();
				}
				catch (Exception x)
				{
					ret = x;
				}
				finally
				{
					mySqlDataReader.Close();
				}

				if (ret is Exception e)
				{
					throw new SafetyException("Unexpected internal server error while fetching ethereum deposit address!", e);
				}
				else if (ret is string str)
				{
					return BlockchainManager.MintME.GetWalletManager(str).address;
				}
				else
				{
					ThrowInternal2("Unexpected type while fetching ethereum deposit address (should not reach here)!");
					return null;
				}
			}

			protected override bool NeedRedis()
			{
				return true;
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.ReadUncommitted;
			}
		}
		private sealed class Login : CaptchaProtectedRequestMethod
		{
			public static readonly RequestMethod instance = new Login();
			private Login()
			{

			}

			public override void Execute2(Request request)
			{
				string username = request.ExtractRequestArg<string>("username");
				string password = request.ExtractRequestArg<string>("password");
				bool remember = request.ExtractRequestArg<bool>("renember");

				MySqlCommand mySqlCommand = request.sqlCommandFactory.GetCommand("SELECT UserID, Passhash FROM Accounts WHERE Username = @username;");
				mySqlCommand.Parameters.AddWithValue("@username", username);
				MySqlDataReader mySqlDataReader = mySqlCommand.ExecuteReader();
				Exception throws = null;
				string hash;
				ulong userid;
				try
				{
					CheckSafety(mySqlDataReader.Read(), "Invalid credentials!");
					hash = mySqlDataReader.GetString("Passhash");
					userid = mySqlDataReader.GetUInt64("UserID");
					mySqlDataReader.CheckSingletonResult();
				}
				catch (Exception e)
				{
					throws = e;
					hash = null;
					userid = 0;
				}
				finally
				{
					mySqlDataReader.Close();
				}

				if (throws is null)
				{
					CheckSafety(OpenBsdBCrypt.CheckPassword(hash, password.ToCharArray()), "Invalid credentials!");
					byte[] SessionToken = new byte[64];
					RandomNumberGenerator randomNumberGenerator = RandomNumberGenerator.Create();
					randomNumberGenerator.GetBytes(SessionToken);
					randomNumberGenerator.Dispose();
					SHA256 sha256 = SHA256.Create();
					string selector = "SESSION_" + BitConverter.ToString(sha256.ComputeHash(SessionToken)).Replace("-", string.Empty);
					sha256.Dispose();
					lock (request.sharedAuthenticationHint.redisClient)
					{
						request.sharedAuthenticationHint.redisClient.SetNx(selector, userid.ToString(), 2592000);
					}

					string cookie = "__Secure-OpenCEX_session =" + WebUtility.UrlEncode(Convert.ToBase64String(SessionToken)) + (remember ? ("; Domain=" + CookieOrigin + "; Max-Age=2592000; Path=/; Secure; HttpOnly; SameSite=None") : ("; Domain=" + CookieOrigin + "; Path=/; Secure; HttpOnly; SameSite=None"));
					lock (request.httpListenerContext)
					{
						request.httpListenerContext.Response.AddHeader("Set-Cookie", cookie);
					}
				}
				else if (throws is SafetyException)
				{
					throw throws;
				}
				else
				{
					throw new SafetyException("Unexpected internal server error while logging in (should not reach here)!", throws);
				}
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.ReadUncommitted;
			}
		}

		private sealed class Withdraw : RequestMethod
		{
			public static readonly RequestMethod instance = new Withdraw();
			private Withdraw()
			{

			}
			public override object Execute(Request request)
			{
				ulong userid = request.GetUserID();
				string token = request.ExtractRequestArg<string>("token");
				string address = request.ExtractRequestArg<string>("address");
				SafeUint amount = request.ExtractSafeUint("amount");
				BlockchainManager blockchainManager;
				switch (token)
				{
					//LP tokens
					case "LP_MATIC_PolyEUBI":
						BurnLP(request.sqlCommandFactory, "MATIC", "PolyEUBI", amount, userid);
						return null;
					case "LP_MintME_MATIC":
						BurnLP(request.sqlCommandFactory, "MintME", "MATIC", amount, userid);
						return null;
					case "LP_MintME_BNB":
						BurnLP(request.sqlCommandFactory, "MintME", "BNB", amount, userid);
						return null;
					case "LP_MintME_CLICK":
						BurnLP(request.sqlCommandFactory, "MintME", "CLICK", amount, userid);
						return null;
					case "LP_MintME_Haoma":
						BurnLP(request.sqlCommandFactory, "MintME", "Haoma", amount, userid);
						return null;
					case "LP_MintME_MS-Coin":
						BurnLP(request.sqlCommandFactory, "MintME", "MS-Coin", amount, userid);
						return null;
					case "LP_MintME_PolyEUBI":
						BurnLP(request.sqlCommandFactory, "MintME", "PolyEUBI", amount, userid);
						return null;
					case "LP_MintME_EUBI":
						BurnLP(request.sqlCommandFactory, "MintME", "EUBI", amount, userid);
						return null;
					case "LP_MintME_1000x":
						BurnLP(request.sqlCommandFactory, "MintME", "1000x", amount, userid);
						return null;
					case "LP_BNB_PolyEUBI":
						BurnLP(request.sqlCommandFactory, "BNB", "PolyEUBI", amount, userid);
						return null;
					case "LP_shitcoin_scamcoin":
						BurnLP(request.sqlCommandFactory, "shitcoin", "scamcoin", amount, userid);
						return null;

					//Traditional tokens
					case "MATIC":
					case "PolyEUBI":
					case "Dai":
						blockchainManager = BlockchainManager.Polygon;
						break;
					case "EUBI":
					case "1000x":
					case "CLICK":
					case "Haoma":
					case "MS-Coin":
						blockchainManager = BlockchainManager.MintME;
						break;
					case "BNB":
						blockchainManager = BlockchainManager.BinanceSmartChain;
						break;

					//Multichain tokens
					case "MintME":
						string blockchain;
						{
							CheckSafety(request.args.TryGetValue("blockchain", out object tm), "Must specify blockchain for multichain token!");
							blockchain = (string)tm;
						}
						blockchainManager = blockchain switch
						{
							"MintME" => BlockchainManager.MintME,
							"Polygon" => BlockchainManager.Polygon,
							_ => throw new SafetyException("Unsupported blockchain!"),
						};
						break;

					//Unsupported tokens
					default:
						throw new SafetyException("Unsupported token!");
				}
				WalletManager walletManager = blockchainManager.ExchangeWalletManager;
				SafeUint gasPrice = walletManager.GetGasPrice();
				//Boost gas price to reduce server waiting time.
				gasPrice = gasPrice.Add(gasPrice.Div(ten));
				string tokenAddress;
				bool backed;
				switch (token)
				{
					case "MS-Coin":
						tokenAddress = "0x34c171a4ee5a3e6ad7ea1b356600e30d7c333d5e";
						backed = false;
						break;
					case "CLICK":
						tokenAddress = "0xf4811b341af177bde2407b976311af66c4b08021";
						backed = false;
						break;
					case "Haoma":
						tokenAddress = "0x60ac85dda46937251eb7473a68a1b9367ee2a1f1";
						backed = false;
						break;
					case "PolyEUBI":
						tokenAddress = "0x553e77f7f71616382b1545d4457e2c1ee255fa7a";
						backed = false;
						break;
					case "EUBI":
						tokenAddress = "0x8afa1b7a8534d519cb04f4075d3189df8a6738c1";
						backed = false;
						break;
					case "1000x":
						tokenAddress = "0x7b535379bbafd9cd12b35d91addabf617df902b2";
						backed = false;
						break;
					case "Dai":
						tokenAddress = "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063";
						backed = false;
						break;
					case "MintME":
						if (blockchainManager.chainid == 24734)
						{
							tokenAddress = null;
							backed = false;
						}
						else
						{
							tokenAddress = "0x2b7bede8a97021da880e6c84e8b915492d2ae216";
							backed = true;
						}
						break;
					default:
						tokenAddress = null;
						backed = false;
						break;
				}

				if (tokenAddress is null)
				{
					//Verify address
					VerifyAddress(address);

					//Estimate gas
					CheckSafety(walletManager.EstimateGas(address, gasPrice, amount, "") == basegas, "Withdraw to contract not supported!");
					SafeUint withfee = amount.Add(gasPrice.Mul(basegas));

					request.Debit(token, userid, withfee, false);

					//Send withdrawal later
					walletManager.Unsafe_SafeSendEther(request.sqlCommandFactory, amount, address, gasPrice, basegas, null, userid, false, token, withfee, token);
				}
				else
				{
					string gastoken;
					if (blockchainManager.chainid == 24734)
					{
						gastoken = "MintME";
					}
					else
					{
						gastoken = "MATIC";
					}

					//Prepare ABI
					string data = "0xa9059cbb" + ExpandABIAddress(address) + amount.ToHex(false, true);

					//Estimate gas
					SafeUint gas = walletManager.EstimateGas(tokenAddress, gasPrice, zero, data);

					//Debit unbacked gas fees
					request.Debit(gastoken, userid, gasPrice.Mul(gas), false);

					request.Debit(token, userid, amount, backed);

					//Send withdrawal later
					walletManager.Unsafe_SafeSendEther(request.sqlCommandFactory, amount, tokenAddress, gasPrice, gas, data, userid, false, token, amount, token);
				}

				return null;
			}

			protected override bool NeedRedis()
			{
				return true;
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.RepeatableRead;
			}
		}
		private abstract class CaptchaProtectedRequestMethod : RequestMethod
		{
			public abstract void Execute2(Request request);

			[JsonObject(MemberSerialization.Fields)]
			private sealed class CaptchaResult
			{
				public bool success = false;
			}

			private static readonly byte[] prefixData1 = Encoding.ASCII.GetBytes("secret=");
			private static readonly byte[] prefixData2 = Encoding.ASCII.GetBytes("&response=");
			private static readonly byte[] bytes1 = HttpUtility.UrlEncodeToBytes(GetEnv("CaptchaSecret"));
			private static readonly JsonSerializerSettings CaptchaValidatorJsonSerializerSettings = new JsonSerializerSettings();

			static CaptchaProtectedRequestMethod()
			{
				CaptchaValidatorJsonSerializerSettings.MaxDepth = 2;
				CaptchaValidatorJsonSerializerSettings.MissingMemberHandling = MissingMemberHandling.Ignore;
			}
			public override object Execute(Request request)
			{
				string temp = request.ExtractRequestArg<string>("captcha");

				WebRequest httpWebRequest = WebRequest.Create("https://www.google.com/recaptcha/api/siteverify");
				httpWebRequest.Method = "POST";
				httpWebRequest.ContentType = "application/x-www-form-urlencoded";

				byte[] bytes2 = HttpUtility.UrlEncodeToBytes(temp);

				using (Stream stream = httpWebRequest.GetRequestStream())
				{
					stream.Write(prefixData1, 0, 7);
					stream.Write(bytes1, 0, bytes1.Length);
					stream.Write(prefixData2, 0, 10);
					stream.Write(bytes2, 0, bytes2.Length);
					stream.Flush();
				}

				string returns;
				using (WebResponse webResponse = httpWebRequest.GetResponse())
				{
					returns = new StreamReader(webResponse.GetResponseStream()).ReadToEnd();
				}
				CheckSafety(JsonConvert.DeserializeObject<CaptchaResult>(returns, CaptchaValidatorJsonSerializerSettings).success, "Invalid captcha!");

				Execute2(request);
				return null;
			}

			protected override bool NeedRedis()
			{
				return true;
			}
		}
		private sealed class CreateAccount : CaptchaProtectedRequestMethod
		{
			public static readonly RequestMethod instance = new CreateAccount();
			private CreateAccount()
			{

			}

			public override void Execute2(Request request)
			{
				CheckSafety2(debug, "Account creation not allowed on dev server!");
				string username;
				string password;
				{
					CheckSafety(request.args.TryGetValue("username", out object temp), "Missing username!");
					username = (string)temp;
					CheckSafety(request.args.TryGetValue("password", out temp), "Missing password!");
					password = (string)temp;
				}
				CheckSafety2(username.Length < 5, "Excessively short username!");
				CheckSafety2(username.Length > 255, "Excessively long username!");
				CheckSafety2(password.Length > 72, "Excessively long password!");
				byte[] salt = new byte[16];
				byte[] privatekey = new byte[32];
				RandomNumberGenerator rng = RandomNumberGenerator.Create();
				rng.GetBytes(salt);
				rng.GetBytes(privatekey);
				rng.Dispose();

				MySqlCommand mySqlCommand = request.sqlCommandFactory.GetCommand("INSERT INTO Accounts (Username, Passhash, DepositPrivateKey) VALUES (@username, @passhash, \"" + BitConverter.ToString(privatekey).Replace("-", string.Empty).ToLower() + "\");");
				mySqlCommand.Parameters.AddWithValue("@username", username);
				mySqlCommand.Parameters.AddWithValue("@passhash", OpenBsdBCrypt.Generate(password.ToCharArray(), salt, 16));
				try
				{
					mySqlCommand.SafeExecuteNonQuery();
				}
				catch
				{
					throw new SafetyException("Username not available!");
				}


				//HACK: Hijack existing request method
				request.args.Add("renember", true);
				((Login)Login.instance).Execute2(request);
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.RepeatableRead;
			}
		}

		private sealed class Logout : RequestMethod
		{
			public static readonly RequestMethod instance = new Logout();
			private Logout()
			{

			}
			public override object Execute(Request request)
			{
				Cookie cookie = request.httpListenerContext.Request.Cookies["__Secure-OpenCEX_session"];
				if (cookie != null)
				{
					byte[] bytes;
					try
					{
						bytes = Convert.FromBase64String(WebUtility.UrlDecode(cookie.Value));
					}
					catch
					{
						return null;
					}
					SHA256 hash = SHA256.Create();
					bytes = hash.ComputeHash(bytes);
					hash.Dispose();
					lock (request.sharedAuthenticationHint.redisClient)
					{
						request.sharedAuthenticationHint.redisClient.Del("SESSION_" + BitConverter.ToString(bytes).Replace("-", string.Empty));
					}
				}
				return null;
			}

			protected override bool NeedRedis()
			{
				return true;
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.Unspecified;
			}
		}

		private sealed class LoadActiveOrders : RequestMethod
		{
			public static readonly RequestMethod instance = new LoadActiveOrders();
			private LoadActiveOrders()
			{

			}
			public override object Execute(Request request)
			{
				Queue<object[]> objects = new Queue<object[]>();
				MySqlDataReader mySqlDataReader = request.sqlCommandFactory.GetCommand("SELECT Pri, Sec, Price, InitialAmount, TotalCost, Id, Buy FROM Orders WHERE PlacedBy = " + request.GetUserID() + ";").ExecuteReader();
				Exception throwlater = null;
				try
				{
					while (mySqlDataReader.Read())
					{
						object[] buffer = new object[7];
						buffer[0] = mySqlDataReader.GetString("Pri");
						buffer[1] = mySqlDataReader.GetString("Sec");
						buffer[2] = mySqlDataReader.GetString("Price");
						buffer[3] = mySqlDataReader.GetString("InitialAmount");
						buffer[4] = mySqlDataReader.GetString("TotalCost");
						buffer[5] = mySqlDataReader.GetUInt64("Id").ToString();
						buffer[6] = mySqlDataReader.GetBoolean("Buy");
						objects.Enqueue(buffer);
					}
				}
				catch (Exception e)
				{
					throwlater = e;
				}
				mySqlDataReader.Close();

				if (throwlater is null)
				{
					return objects.ToArray();
				}
				else
				{
					throw new SafetyException("Unable to fetch active orders!", throwlater);
				}

			}

			protected override bool NeedRedis()
			{
				return true;
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.ReadUncommitted;
			}
		}

		private sealed class GetChart : RequestMethod
		{
			public static readonly RequestMethod instance = new GetChart();
			private GetChart()
			{

			}

			[JsonObject(MemberSerialization.Fields)]
			private sealed class Candle
			{
				public readonly ulong x;
				public readonly string o;
				public readonly string h;
				public readonly string l;
				public readonly string c;

				public Candle(ulong x, string o, string h, string l, string c)
				{
					this.x = x;
					this.o = o ?? throw new ArgumentNullException(nameof(o));
					this.h = h ?? throw new ArgumentNullException(nameof(h));
					this.l = l ?? throw new ArgumentNullException(nameof(l));
					this.c = c ?? throw new ArgumentNullException(nameof(c));
				}
			}
			public override object Execute(Request request)
			{
				string pri;
				string sec;
				{
					CheckSafety(request.args.TryGetValue("primary", out object temp));
					pri = (string)temp;
					CheckSafety(request.args.TryGetValue("secondary", out temp));
					sec = (string)temp;
				}
				try
				{
					GetEnv("PairExists_" + pri.Replace("_", "__") + "_" + sec);
				}
				catch
				{
					throw new SafetyException("Non-existant trading pair!");
				}

				Queue<Candle> objects = new Queue<Candle>();
				MySqlCommand mySqlCommand = request.sqlCommandFactory.GetCommand("SELECT Timestamp, Open, High, Low, Close, Timestamp FROM HistoricalPrices WHERE Pri = @primary AND Sec = @secondary ORDER BY Timestamp ASC LIMIT 60;");
				mySqlCommand.Parameters.AddWithValue("@primary", pri);
				mySqlCommand.Parameters.AddWithValue("@secondary", sec);
				mySqlCommand.Prepare();
				MySqlDataReader mySqlDataReader = mySqlCommand.ExecuteReader();
				Exception throwlater = null;
				try
				{
					while (mySqlDataReader.Read())
					{
						objects.Enqueue(new Candle(Convert.ToUInt64(mySqlDataReader.GetString("Timestamp")), mySqlDataReader.GetString("Open"), mySqlDataReader.GetString("High"), mySqlDataReader.GetString("Low"), mySqlDataReader.GetString("Close")));
					}
				}
				catch (Exception e)
				{
					throwlater = e;
				}
				mySqlDataReader.Close();

				if (throwlater is null)
				{
					return objects.ToArray();
				}
				else
				{
					throw new SafetyException("Unable to fetch price chart!", throwlater);
				}
			}

			protected override bool NeedRedis()
			{
				return false;
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.ReadUncommitted;
			}
		}
		private sealed class MintLP1 : RequestMethod
		{
			private MintLP1()
			{

			}
			public static readonly RequestMethod instance = new MintLP1();
			public override object Execute(Request request)
			{
				string pri;
				string sec;
				SafeUint amount0;
				SafeUint amount1;
				{
					CheckSafety(request.args.TryGetValue("primary", out object temp), "Missing primary token!");
					pri = (string)temp;
					CheckSafety(request.args.TryGetValue("secondary", out temp), "Missing secondary token!");
					sec = (string)temp;
					CheckSafety(request.args.TryGetValue("amount0", out temp), "Missing primary amount!");
					amount0 = GetSafeUint((string)temp);
					CheckSafety(request.args.TryGetValue("amount1", out temp), "Missing secondary amount!");
					amount1 = GetSafeUint((string)temp);
				}
				try
				{
					GetEnv("PairExists_" + pri.Replace("_", "__") + "_" + sec);
				}
				catch
				{
					throw new SafetyException("Nonexistant trading pair!");
				}
				LPReserve lpreserve = new LPReserve(request.sqlCommandFactory, pri, sec);

				if (!(lpreserve.reserve0.isZero || lpreserve.reserve1.isZero))
				{
					SafeUint optimal1 = lpreserve.QuoteLP(amount0, true);
					if (optimal1 > amount1)
					{
						SafeUint optimal0 = lpreserve.QuoteLP(amount1, false);
						CheckSafety2(optimal0 > amount0, "Uniswap.NET: Insufficent primary amount (should not reach here)!", true);
						amount0 = optimal0;
					}
					else
					{
						amount1 = optimal1;
					}
				}

				MintLP(request.sqlCommandFactory, pri, sec, amount0, amount1, request.GetUserID(), lpreserve);
				return null;
			}

			protected override bool NeedRedis()
			{
				return true;
			}
			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.RepeatableRead;
			}
		}
		private sealed class PostWithdrawal : ConcurrentJob
		{
			private readonly WalletManager walletManager1;
			private readonly string tx;

			private readonly ulong userid;
			private readonly string coin;
			private readonly SafeUint refundIfFail;
			private readonly bool backed;

			public PostWithdrawal(WalletManager walletManager1, string tx, ulong userid, string coin, SafeUint refundIfFail, bool b)
			{
				this.walletManager1 = walletManager1 ?? throw new ArgumentNullException(nameof(walletManager1));
				this.tx = tx ?? throw new ArgumentNullException(nameof(tx));
				this.userid = userid;
				this.coin = coin ?? throw new ArgumentNullException(nameof(coin));
				this.refundIfFail = refundIfFail ?? throw new ArgumentNullException(nameof(refundIfFail));
				backed = b;
			}

			protected override object ExecuteIMPL()
			{
				try
				{
					walletManager1.SendRawTX(tx);
				}
				catch (Exception e)
				{
					Console.Error.WriteLine("Exception while sending withdrawal: " + e.ToString());
					SQLCommandFactory sql = GetSQL(IsolationLevel.RepeatableRead);
					bool commit;
					try
					{
						sql.Credit(coin, userid, refundIfFail, backed);
						commit = true;
					}
					catch (Exception x)
					{
						commit = false;
						Console.Error.WriteLine("Exception while crediting failed withdrawal back to user: " + x.ToString());
						Console.Error.WriteLine("UnsafeCredit " + coin.ToString() + " " + userid.ToString() + " " + refundIfFail.ToString());
						sql.DestroyTransaction(false, true);
					}
					if (commit)
					{
						try
						{
							sql.DestroyTransaction(true, true);
						}
						catch (Exception x)
						{
							Console.Error.WriteLine("Exception while crediting failed withdrawal back to user: " + x.ToString());
							Console.Error.WriteLine("UnsafeCredit " + coin.ToString() + " " + userid.ToString() + " " + refundIfFail.ToString());
						}
					}
				}

				return null;
			}
		}

		private sealed class MintDerivatives : RequestMethod
		{
			public static readonly RequestMethod instance = new MintDerivatives();
			private MintDerivatives()
			{

			}
			public override object Execute(Request request)
			{
				ulong userid = request.GetUserID();
				string contract = request.ExtractRequestArg<string>("contract");
				try
				{
					GetEnv("IsDerivative_" + contract);
				}
				catch
				{
					throw new SafetyException("Invalid derivatives!");
				}
				int pivot = contract.LastIndexOf('_');
				IDerivativeContract derivativeType = contract[pivot..] switch
				{
					"_PUT" => PutOption.instance,
					_ => throw new SafetyException("Unknown derivative type!"),
				};
				SafeUint amount = request.ExtractSafeUint("amount");
				MySqlCommand command = request.sqlCommandFactory.GetCommand("SELECT Strike FROM Derivatives WHERE Name = @coin FOR UPDATE;");
				command.Parameters.AddWithValue("@coin", contract);
				command.Prepare();
				SafeUint strike;
				using (MySqlDataReader tmpreader2 = command.ExecuteReader())
				{
					CheckSafety(tmpreader2.Read(), "Missing derivatives record!");
					strike = GetSafeUint(tmpreader2.GetString("Strike"));
					tmpreader2.CheckSingletonResult();
				}
				request.Debit("Dai", userid, amount.Mul(derivativeType.CalculateMaxShortLoss(strike)).Div(ether), true);

				//Create new derivatives contracts
				request.Credit(contract, userid, amount, false);
				request.Credit(contract + "_SHORT", userid, amount, false);
				return null;
			}

			protected override bool NeedRedis()
			{
				return true;
			}

			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.RepeatableRead;
			}
		}
		private sealed class GetDerivativeInfo : RequestMethod
		{
			public static readonly RequestMethod instance = new GetDerivativeInfo();
			private GetDerivativeInfo()
			{

			}
			public override object Execute(Request request)
			{
				MySqlCommand cmd = request.sqlCommandFactory.GetCommand("SELECT Expiry, Strike FROM Derivatives WHERE Name = @name;");
				cmd.Parameters.AddWithValue("@name", request.ExtractRequestArg<string>("contract"));
				using(MySqlDataReader reader = cmd.ExecuteReader()){
					CheckSafety(reader.Read(), "Invalid derivatives!");
					object tmp = new object[] {reader.GetUInt64("Expiry"), reader.GetString("Strike")};
					CheckSafety2(reader.Read(), "Duplicate derivatives records!");
					return tmp;
				}
			}

			protected override bool NeedRedis()
			{
				return false;
			}

			protected override IsolationLevel SQLMode()
			{
				return IsolationLevel.ReadUncommitted;
			}
		}
	}
}
