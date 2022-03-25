using jessielesbian.OpenCEX;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using jessielesbian.OpenCEX.RequestManager;
using jessielesbian.OpenCEX.SafeMath;
using MySql.Data.MySqlClient;
using System.Text;

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
					fillMode = (long) tmp;
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
				try{
					GetEnv("PairExists_" + primary.Replace("_", "__") + "_" + secondary);
				} catch{
					throw new SafetyException("Nonexistant trading pair!");
				}

				string selected;
				string output;
				SafeUint amt2;
				MySqlCommand counter;
				if(buy){
					selected = primary;
					output = secondary;
					amt2 = amount.Mul(price).Div(ether);
					counter = request.sqlCommandFactory.GetCommand("SELECT Price, Amount, InitialAmount, TotalCost, Id, PlacedBy FROM Orders WHERE Pri = @primary AND Sec = @secondary AND Buy = 0 ORDER BY Price ASC, Id ASC FOR UPDATE;");
				} else{
					selected = secondary;
					output = primary;
					amt2 = amount;
					counter = request.sqlCommandFactory.GetCommand("SELECT Price, Amount, InitialAmount, TotalCost, Id, PlacedBy FROM Orders WHERE Pri = @primary AND Sec = @secondary AND Buy = 1 ORDER BY Price DESC, Id ASC FOR UPDATE;");
				}
				counter.Parameters.AddWithValue("@primary", primary);
				counter.Parameters.AddWithValue("@secondary", secondary);
				counter.Prepare();

				if (fillMode == 0){
					CheckSafety2(amount.isZero, "Zero limit order size!");
					CheckSafety2(amount < GetSafeUint(GetEnv("MinimumLimit_" + selected)), "Order is smaller than minimum limit order size!");
				}
				

				//Partially-atomic increment
				MySqlDataReader reader = request.sqlCommandFactory.SafeExecuteReader(request.sqlCommandFactory.GetCommand("SELECT Val FROM Misc WHERE Kei = \"OrderCounter\" FOR UPDATE;"));
				ulong orderId;
				if(reader.HasRows){
					orderId = Convert.ToUInt64(reader.GetString("Val")) + 1;
					reader.CheckSingletonResult();
				} else{
					orderId = 0;
				}

				request.sqlCommandFactory.SafeDestroyReader();

				if(orderId == 0){
					request.sqlCommandFactory.SafeExecuteNonQuery("INSERT INTO Misc (Kei, Val) VALUES (\"OrderCounter\", \"0\");");
				} else{
					request.sqlCommandFactory.SafeExecuteNonQuery("UPDATE Misc SET Val = \"" + orderId + "\"WHERE Kei = \"OrderCounter\";");
				}

				ulong userid = request.GetUserID();
				request.Debit(selected, userid, amount);

				reader = request.sqlCommandFactory.SafeExecuteReader(counter);

				Queue<Order> moddedOrders = new Queue<Order>();
				Order instance = new Order(price, amt2, amount, zero, userid, orderId.ToString());
				Dictionary<ulong, SafeUint> tmpbalances = new Dictionary<ulong, SafeUint>();
				SafeUint debt = zero;
				if(reader.HasRows){
					bool read = true;
					while(read)
					{
						Order other = new Order(GetSafeUint(reader.GetString("Price")), GetSafeUint(reader.GetString("Amount")), GetSafeUint(reader.GetString("InitialAmount")), GetSafeUint(reader.GetString("TotalCost")), reader.GetUInt64("PlacedBy"), reader.GetString("Id"));
						SafeUint temp2 = MatchOrders(instance, other, buy);
						if(temp2.isZero){
							break;
						} else{
							moddedOrders.Enqueue(other);
							
							if (buy){
								SafeUint secamt = temp2.Mul(other.price).Div(ether);
								debt = debt.Add(temp2);
								if(tmpbalances.TryGetValue(other.placedby, out SafeUint temp3)){
									tmpbalances[other.placedby] = temp3.Add(secamt);
								} else{
									tmpbalances.Add(other.placedby, secamt);
								}
							} else{
								debt = debt.Add(secamt);
								if (tmpbalances.TryGetValue(other.placedby, out SafeUint temp3))
								{
									tmpbalances[other.placedby] = temp3.Add(temp2);
								}
								else
								{
									tmpbalances.Add(other.placedby, temp2);
								}
							}
							read = reader.Read();
						}
					}
				}

				request.sqlCommandFactory.SafeDestroyReader();

				if(!instance.Balance.isZero)
				{
					//We only save the order to database if it's a limit order and it's not fully executed.
					CheckSafety2(fillMode == 2, "Fill or kill order canceled due to insufficient liquidity!");
					if(instance.amount == zero || fillMode == 1){
						//Cancel order
						request.Credit(selected, userid, instance.Balance);
						goto admitted;
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
						if(!balance.isZero){
							request.Credit(output, modded.placedby, balance);
						}
						
						request.sqlCommandFactory.SafeExecuteNonQuery("DELETE FROM Orders WHERE Id = \"" + modded.id + "\";");
					}
					else
					{
						request.sqlCommandFactory.SafeExecuteNonQuery("UPDATE Orders SET Amount = \"" + modded.amount + "\", TotalCost = \"" + modded.totalCost + "\"" + " WHERE Id = \"" + modded.id + "\";");
					}

				}

				//Credit funds to customers
				if (!debt.isZero)
				{
					request.Credit(output, userid, debt);
				}

				foreach(KeyValuePair<ulong, SafeUint> keyValuePair in tmpbalances){
					request.Credit(selected, keyValuePair.Key, keyValuePair.Value);
				}

				return null;
			}

			private static SafeUint MatchOrders(Order first, Order second, bool buy){
				SafeUint ret = first.amount.Min(second.amount);
				if (buy){
					if(second.price > first.price){
						return zero;
					} else{
						first.Debit(ret);
						second.Debit(ret, second.price);
					}
				} else{
					if (first.price > second.price)
					{
						return zero;
					} else{
						first.Debit(ret, second.price);
						second.Debit(ret);
					}
				}
				CheckSafety2(ret.isZero, "Order matched without output (should not reach here)!");
				return ret;
			}

			protected override bool NeedSQL()
			{
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

				public SafeUint Balance => initialAmount.Sub(totalCost);
			}
		}
	}
}
