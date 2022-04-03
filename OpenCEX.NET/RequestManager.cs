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

namespace jessielesbian.OpenCEX.RequestManager
{
	public sealed class Request : ConcurrentJob
	{
		public readonly RequestMethod method;
		public readonly HttpListenerContext httpListenerContext;
		public readonly IDictionary<string, object> args;
		public readonly SQLCommandFactory sqlCommandFactory;

		public Request(SQLCommandFactory sqlCommandFactory, RequestMethod method, HttpListenerContext httpListenerContext, IDictionary<string, object> args)
		{
			if(method.needSQL){
				this.sqlCommandFactory = sqlCommandFactory ?? throw new ArgumentNullException(nameof(sqlCommandFactory));
			} else{
				this.sqlCommandFactory = null;
			}
			
			this.method = method ?? throw new ArgumentNullException(nameof(method));
			this.httpListenerContext = httpListenerContext ?? throw new ArgumentNullException(nameof(httpListenerContext));
			this.args = args ?? throw new ArgumentNullException(nameof(args));
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
				} finally{
					if (StaticUtils.debug)
					{
						throw new SafetyException("Unable to execute request", e);
					}
					else
					{
						throw e;
					}
				}	
			}
			return ret;
			

		}
	}

	public abstract class RequestMethod{
		public abstract object Execute(Request request);
		protected abstract bool NeedSQL();
		public readonly bool needSQL;

		public RequestMethod(){
			needSQL = NeedSQL();
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

			protected override bool NeedSQL()
			{
				return true;
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
				try{
					target = Convert.ToUInt64(target2);
				} catch{
					throw new SafetyException("Target must be unsigned number!");
				}

				ulong userid = request.GetUserID();

				MySqlDataReader mySqlDataReader = request.sqlCommandFactory.SafeExecuteReader(request.sqlCommandFactory.GetCommand("SELECT PlacedBy, Pri, Sec, InitialAmount, TotalCost, Buy FROM Orders WHERE Id = \"" + target + "\" FOR UPDATE;"));
				CheckSafety(mySqlDataReader.HasRows, "Nonexistant order!");
				CheckSafety(mySqlDataReader.GetUInt64("PlacedBy") == userid, "Attempted to cancel another user's order!");
				string refund;
				if(mySqlDataReader.GetUInt32("Buy") == 0){
					refund = mySqlDataReader.GetString("Sec");
				} else{
					refund = mySqlDataReader.GetString("Pri");
				}
				SafeUint amount = GetSafeUint(mySqlDataReader.GetString("InitialAmount")).Sub(GetSafeUint(mySqlDataReader.GetString("TotalCost")));

				mySqlDataReader.CheckSingletonResult();
				request.sqlCommandFactory.SafeDestroyReader();

				request.sqlCommandFactory.SafeExecuteNonQuery("DELETE FROM Orders WHERE Id = \"" + target + "\";");
				request.Credit(refund, userid, amount);
				return null;
			}

			protected override bool NeedSQL()
			{
				return true;
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
				long fillMode;
				SafeUint price;
				SafeUint amount;
				string primary;
				string secondary;
				bool buy;
				{
					object tmp = null;
					CheckSafety(request.args.TryGetValue("fill_mode", out tmp), "Missing order fill mode!");
					fillMode = (long)tmp;
					CheckSafety(request.args.TryGetValue("price", out tmp), "Missing order price!");
					price = GetSafeUint((string)tmp);
					CheckSafety(request.args.TryGetValue("amount", out tmp), "Missing order amount!");
					amount = GetSafeUint((string)tmp);
					CheckSafety(request.args.TryGetValue("primary", out tmp), "Missing primary token!");
					primary = (string)tmp;
					CheckSafety(request.args.TryGetValue("secondary", out tmp), "Missing secondary token!");
					secondary = (string)tmp;
					CheckSafety(request.args.TryGetValue("buy", out tmp), "Missing order type!");
					buy = Convert.ToBoolean(tmp);
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
				SafeUint amt2;
				MySqlCommand counter;
				if (buy)
				{
					selected = primary;
					output = secondary;
					amt2 = amount.Mul(ether).Div(price);
					counter = request.sqlCommandFactory.GetCommand("SELECT Price, Amount, InitialAmount, TotalCost, Id, PlacedBy FROM Orders WHERE Pri = @primary AND Sec = @secondary AND Buy = 0 ORDER BY Price DESC, Id ASC FOR UPDATE;");
				}
				else
				{
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

				//Partially-atomic increment
				MySqlDataReader reader = request.sqlCommandFactory.SafeExecuteReader(request.sqlCommandFactory.GetCommand("SELECT Val FROM Misc WHERE Kei = \"OrderCounter\" FOR UPDATE;"));
				ulong orderId;
				if (reader.HasRows)
				{
					orderId = Convert.ToUInt64(reader.GetString("Val")) + 1;
					reader.CheckSingletonResult();
				}
				else
				{
					orderId = 0;
				}

				request.sqlCommandFactory.SafeDestroyReader();

				if (orderId == 0)
				{
					request.sqlCommandFactory.SafeExecuteNonQuery("INSERT INTO Misc (Kei, Val) VALUES (\"OrderCounter\", \"0\");");
				}
				else
				{
					request.sqlCommandFactory.SafeExecuteNonQuery("UPDATE Misc SET Val = \"" + orderId + "\"WHERE Kei = \"OrderCounter\";");
				}

				LPReserve lpreserve = new LPReserve(request.sqlCommandFactory, primary, secondary);
				
				Queue<Order> moddedOrders = new Queue<Order>();
				Dictionary<ulong, SafeUint> tmpbalances = new Dictionary<ulong, SafeUint>();
				SafeUint close = null;

				ulong userid = request.GetUserID();
				request.Debit(selected, userid, amount);
				request.Credit(output, userid, zero);
				reader = request.sqlCommandFactory.SafeExecuteReader(counter);
				Order instance = new Order(price, amt2, amount, zero, userid, orderId.ToString());
				if (reader.HasRows)
				{
					bool read = true;
					while (read)
					{
						Order other = new Order(GetSafeUint(reader.GetString("Price")), GetSafeUint(reader.GetString("Amount")), GetSafeUint(reader.GetString("InitialAmount")), GetSafeUint(reader.GetString("TotalCost")), reader.GetUInt64("PlacedBy"), reader.GetString("Id"));
						SafeUint oldamt1 = instance.Balance;
						SafeUint oldamt2 = other.Balance;

						
						SafeUint arbitrageAmount = ComputeProfitMaximizingTrade(price, lpreserve, out bool arbitrageBuy).Min(other.Balance);
						if(!arbitrageAmount.isZero && arbitrageBuy == buy){
							//Unbacked flashminting arbitrage - print and burn money in same transaction
							request.Credit(output, userid, arbitrageAmount, false);
							lpreserve = request.sqlCommandFactory.SwapLP(primary, secondary, userid, arbitrageAmount, buy, lpreserve, false, out SafeUint output2);
							SafeUint maxout = other.MaxOutput(buy).Min(output2);
							if(buy){
								other.Debit(maxout);
							} else{
								other.Debit(maxout.Mul(ether).Div(other.price), other.price);
							}
							if(tmpbalances.TryGetValue(other.placedby, out SafeUint premod2)){
								tmpbalances[other.placedby] = premod2.Add(maxout);
							} else{
								tmpbalances.Add(other.placedby, maxout);
							}
							request.Credit(output, userid, oldamt2.Sub(other.Balance).Add(output2.Sub(maxout)));
							try
							{
								request.Debit(output, userid, arbitrageAmount, false);
							} catch{
								throw new SafetyException("Flashmint not repaid (should not reach here)!");
							}
							
						}
						if (oldamt1.isZero || instance.amount.isZero)
						{
							break;
						}
						else if (MatchOrders(instance, other, buy))
						{
							moddedOrders.Enqueue(other);
							close = other.price;
							SafeUint outamt = oldamt1.Sub(instance.Balance);
							request.Credit(output, userid, oldamt2.Sub(other.Balance));
							if (tmpbalances.TryGetValue(other.placedby, out SafeUint temp3))
							{
								tmpbalances[other.placedby] = temp3.Add(outamt);
							}
							else
							{
								tmpbalances.Add(other.placedby, outamt);
							}
							read = reader.Read();
						}
						else
						{
							break;
						}
					}
				}

				request.sqlCommandFactory.SafeDestroyReader();

				if (!instance.Balance.isZero)
				{
					//We only save the order to database if it's a limit order and it's not fully executed.
					if (instance.amount.isZero || fillMode == 1)
					{
						//Cancel order
						request.Credit(selected, userid, instance.Balance);
						goto admitted;
					}
					else
					{
						CheckSafety2(fillMode == 2, "Fill or kill order canceled due to insufficient liquidity!");
					}
					StringBuilder stringBuilder = new StringBuilder("INSERT INTO Orders (Pri, Sec, Price, Amount, InitialAmount, TotalCost, Id, PlacedBy, Buy) VALUES (@primary, @secondary, \"");
					stringBuilder.Append(instance.price.ToString() + "\", \"");
					stringBuilder.Append(instance.amount.ToString() + "\", \"");
					stringBuilder.Append(amount.ToString() + "\", \"");
					stringBuilder.Append(instance.totalCost.ToString() + "\", \"");
					stringBuilder.Append(instance.id + "\", \"");
					stringBuilder.Append(userid.ToString() + (buy ? "\", 1);" : "\", 0);"));
					MySqlCommand mySqlCommand = request.sqlCommandFactory.GetCommand(stringBuilder.ToString());
					mySqlCommand.Parameters.AddWithValue("@primary", primary);
					mySqlCommand.Parameters.AddWithValue("@secondary", secondary);
					mySqlCommand.Prepare();
					mySqlCommand.ExecuteNonQuery();
				}

			admitted:

				while (moddedOrders.TryDequeue(out Order modded))
				{
					if (modded.amount.isZero)
					{
						SafeUint balance = modded.Balance;
						if (!balance.isZero)
						{
							request.Credit(output, modded.placedby, balance);
						}

						request.sqlCommandFactory.SafeExecuteNonQuery("DELETE FROM Orders WHERE Id = \"" + modded.id + "\";");
					}
					else if (modded.Balance.isZero)
					{
						request.sqlCommandFactory.SafeExecuteNonQuery("DELETE FROM Orders WHERE Id = \"" + modded.id + "\";");
					}
					else
					{
						request.sqlCommandFactory.SafeExecuteNonQuery("UPDATE Orders SET Amount = \"" + modded.amount.ToString() + "\", TotalCost = \"" + modded.totalCost.ToString() + "\"" + " WHERE Id = \"" + modded.id + "\";");
					}

				}

				//Credit funds to customers
				foreach (KeyValuePair<ulong, SafeUint> keyValuePair in tmpbalances)
				{
					request.Credit(selected, keyValuePair.Key, keyValuePair.Value);
				}

				//Flush Uniswap.NET
				WriteLP(request.sqlCommandFactory, primary, secondary, lpreserve);

				//Update charts (NOTE: this is ported from OpenCEX PHP server)
				if (close != null)
				{
					MySqlCommand prepared = request.sqlCommandFactory.GetCommand("SELECT Timestamp, Open, High, Low, Close FROM HistoricalPrices WHERE Pri = @primary AND Sec = @secondary ORDER BY Timestamp DESC FOR UPDATE;");
					prepared.Parameters.AddWithValue("@primary", primary);
					prepared.Parameters.AddWithValue("@secondary", secondary);
					prepared.Prepare();
					reader = request.sqlCommandFactory.SafeExecuteReader(prepared);
					SafeUint start = new SafeUint(new BigInteger(DateTimeOffset.Now.ToUnixTimeSeconds()));
					start = start.Sub(start.Mod(day));
					SafeUint high;
					SafeUint low;
					SafeUint open;
					SafeUint time;
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

					request.sqlCommandFactory.SafeDestroyReader();

					if (append)
					{
						prepared = request.sqlCommandFactory.GetCommand("INSERT INTO HistoricalPrices (Open, High, Low, Close, Timestamp, Pri, Sec) VALUES (@open, @high, @low, @close, @timestamp, @primary, @secondary);");
					}
					else
					{
						prepared = request.sqlCommandFactory.GetCommand("UPDATE HistoricalPrices SET Open = @open, High = @high, Low = @low, Close = @close WHERE Timestamp = @timestamp AND Pri = @primary AND Sec = @secondary;");
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
					CheckSafety(prepared.ExecuteNonQuery() == 1, "Excessive write effect!");
				}

				return null;
			}
			protected override bool NeedSQL()
			{
				return true;
			}
		}

		private static bool MatchOrders(Order first, Order second, bool buy){
			SafeUint ret = first.amount.Min(second.amount);
			if (buy){
				ret = ret.Min(first.Balance.Mul(ether).Div(second.price)).Min(second.Balance);
				if(second.price > first.price){
					return false;
				} else{
					first.Debit(ret, second.price);
					second.Debit(ret);
				}
			} else{
				ret = ret.Min(first.Balance).Min(second.Balance.Mul(ether).Div(second.price));
				if (first.price > second.price)
				{
					return false;
				} else{
					first.Debit(ret);
					second.Debit(ret, second.price);
				}
			}
			CheckSafety2(ret.isZero, "Order matched without output (should not reach here)!");
			return true;
		}

		private class Order
		{
			public readonly SafeUint price;
			public SafeUint amount;
			public readonly SafeUint initialAmount;
			public SafeUint totalCost;
			public readonly ulong placedby;
			public readonly string id;

			public Order(SafeUint price, SafeUint amount, SafeUint initialAmount, SafeUint totalCost, ulong placedby, string id)
			{
				this.initialAmount = initialAmount ?? throw new ArgumentNullException(nameof(initialAmount));
				this.totalCost = totalCost ?? throw new ArgumentNullException(nameof(totalCost));
				this.price = price ?? throw new ArgumentNullException(nameof(price));
				this.amount = amount ?? throw new ArgumentNullException(nameof(amount));
				this.id = id ?? throw new ArgumentNullException(nameof(id));
				this.placedby = placedby;
			}
			public void Debit(SafeUint amt, SafeUint price = null)
			{
				SafeUint temp;
					
				if(price is null){
					temp = totalCost.Add(amt);
				} else{
					temp = totalCost.Add(amt.Mul(price).Div(ether));
				}
				CheckSafety2(temp > initialAmount, "Negative order size (should not reach here)!");
				amount = amount.Sub(amt, "Negative order amount (should not reach here)!");
				totalCost = temp;
			}

			public SafeUint MaxOutput(bool sell){
				if(sell)
				{
					return Balance.Mul(price).Div(ether);
				} else{
					return Balance.Mul(ether).Div(price);
				}
			}

			public SafeUint Balance => initialAmount.Sub(totalCost);
		}

		//Ported from PHP server
		private static SafeUint GetBidOrAsk(SQLCommandFactory sqlCommandFactory, string pri, string sec, bool bid){
			MySqlCommand mySqlCommand;
			if(bid){
				mySqlCommand = sqlCommandFactory.GetCommand("SELECT Price FROM Orders WHERE Pri = @primary AND Sec = @secondary AND Buy = 1 ORDER BY Price DESC LIMIT 1;");
			} else{
				mySqlCommand = sqlCommandFactory.GetCommand("SELECT Price FROM Orders WHERE Pri = @primary AND Sec = @secondary AND Buy = 0 ORDER BY Price ASC LIMIT 1;");
			}

			mySqlCommand.Parameters.AddWithValue("@primary", pri);
			mySqlCommand.Parameters.AddWithValue("@secondary", sec);
			mySqlCommand.Prepare();
			MySqlDataReader reader = sqlCommandFactory.SafeExecuteReader(mySqlCommand);
			SafeUint returns;
			if (reader.HasRows){
				returns = GetSafeUint(reader.GetString("Price"));
				reader.CheckSingletonResult();
				
			} else{
				returns = null;
			}
			sqlCommandFactory.SafeDestroyReader();
			return returns;
		}

		private sealed class BidAsk : RequestMethod
		{
			public static readonly RequestMethod instance = new BidAsk();
			private BidAsk(){
				
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
				
				return new string[] {SafeSerializeSafeUint(GetBidOrAsk(request.sqlCommandFactory, pri, sec, true)), SafeSerializeSafeUint(GetBidOrAsk(request.sqlCommandFactory, pri, sec, false))};
			}

			protected override bool NeedSQL()
			{
				return true;
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
				switch(token){
					case "MintME":
					case "EUBI":
					case "1000x":
						blockchainManager = BlockchainManager.MintME;
						break;
					case "MATIC":
					case "PolyEUBI":
						blockchainManager = BlockchainManager.Polygon;
						break;
					case "BNB":
						blockchainManager = BlockchainManager.BinanceSmartChain;
						break;
					default:
						throw new SafetyException("Unknown token!");
				}
				ulong userid = request.GetUserID();

				MySqlDataReader reader = request.sqlCommandFactory.SafeExecuteReader(request.sqlCommandFactory.GetCommand("SELECT DepositPrivateKey FROM Accounts WHERE UserID = " + userid + ";"));
				WalletManager walletManager = blockchainManager.GetWalletManager(reader.GetString("DepositPrivateKey"));
				reader.CheckSingletonResult();
				request.sqlCommandFactory.SafeDestroyReader();

				string token_address;
				switch(token){
					case "1000x":
						token_address = "0x7b535379bbafd9cd12b35d91addabf617df902b2";
						break;
					case "EUBI":
						token_address = "0x8afa1b7a8534d519cb04f4075d3189df8a6738c1";
						break;
					case "PolyEUBI":
						token_address = "0x553e77f7f71616382b1545d4457e2c1ee255fa7a";
						break;
					default:
						token_address = string.Empty;
						break;
				}
				bool erc20 = token_address != string.Empty;

				SafeUint gasPrice = walletManager.GetGasPrice();

				//Boost gas price to reduce server waiting time.
				gasPrice = gasPrice.Add(gasPrice.Div(ten));
				string txid;
				SafeUint amount;

				if (erc20){
					string formattedTokenAddress = ExpandABIAddress(token_address);
					string postfix = formattedTokenAddress + ExpandABIAddress(walletManager.address);
					walletManager = blockchainManager.ExchangeWalletManager;
					string abi2 = "0xaec6ed90" + ExpandABIAddress(walletManager.address) + postfix;
					
					string ERC20DepositManager;
					string gastoken;
					switch(blockchainManager.chainid)
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
					amount = GetSafeUint(walletManager.Vcall(ERC20DepositManager, gasPrice, zero, abi2));
					string abi = "0x64d7cd50" + postfix + amount.ToHex(false);
					SafeUint gas = walletManager.EstimateGas(ERC20DepositManager, gasPrice, zero, abi);
					request.Debit(gastoken, userid, gas.Mul(gasPrice), false); //Debit gas token to pay for gas
					txid = walletManager.SendEther(zero, ERC20DepositManager, walletManager.SafeNonce(request.sqlCommandFactory), gasPrice, gas, abi);
				} else{
					amount = walletManager.GetEthBalance().Sub(gasPrice.Mul(basegas), "Amount not enough to cover blockchain fee!");
					ulong nonce = walletManager.SafeNonce(request.sqlCommandFactory);
					txid = walletManager.SendEther(amount, ExchangeWalletAddress, nonce, gasPrice, basegas);
				}

				//Re-use existing table for compartiability
				CheckSafety2(amount.isZero, "Zero-value deposit!");
				MySqlCommand mySqlCommand = request.sqlCommandFactory.GetCommand("INSERT INTO WorkerTasks (Status, LastTouched, URL, URL2) VALUES (0, " + userid + ", @token, \"" + txid + "_" + amount.ToString() + "\");");
				mySqlCommand.Parameters.AddWithValue("@token", token);
				mySqlCommand.Prepare();
				CheckSafety(mySqlCommand.ExecuteNonQuery() == 1, "Excessive write effect!");

				//NOTE: the deposits manager will do the rest of the werk for us.
				return null;
			}

			protected override bool NeedSQL()
			{
				return true;
			}
		}
	}
}
