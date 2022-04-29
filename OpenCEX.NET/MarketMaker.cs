using jessielesbian.OpenCEX.SafeMath;
using MySql.Data.MySqlClient;
using System;
using System.Text;

//Uniswap.NET: C# port of the Uniswap Automated Market Maker
//OpenCEX uses Uniswap.NET to optimize capital efficency, and matching engine performance.

namespace jessielesbian.OpenCEX{
	
	public static partial class StaticUtils{
		public struct LPReserve
		{
			public readonly SafeUint reserve0;
			public readonly SafeUint reserve1;
			public readonly SafeUint totalSupply;
			public readonly bool insert;
			public LPReserve(SQLCommandFactory sql, string pri, string sec)
			{
				insert = ReadLP(sql, pri, sec, out reserve0, out reserve1, out totalSupply);
			}

			public LPReserve(SafeUint reserve0, SafeUint reserve1, SafeUint totalSupply, bool insert)
			{
				this.reserve0 = reserve0 ?? throw new ArgumentNullException(nameof(reserve0));
				this.reserve1 = reserve1 ?? throw new ArgumentNullException(nameof(reserve1));
				this.totalSupply = totalSupply ?? throw new ArgumentNullException(nameof(totalSupply));
				this.insert = insert;
			}

			public SafeUint QuoteLP(SafeUint inp, bool atob){
				CheckSafety2(inp.isZero || reserve0.isZero || reserve1.isZero, "Uniswap.NET: Zero liquidity!");
				if(atob){
					return inp.Mul(reserve0).Div(reserve1);
				} else{
					return inp.Mul(reserve1).Div(reserve0);
				}
			}
		}
		public static SafeUint Sqrt(this SafeUint y){
			if (y > three)
			{
				SafeUint z = y;
				SafeUint x = y.Div(two).Add(one);
				while (x < z)
				{
					z = x;
					x = y.Div(x).Add(x).Div(two);
				}
				return z;
			}
			else if (y.isZero)
			{
				return zero;
			} else{
				return one;
			}
		}
		private static bool ReadLP(SQLCommandFactory sql, string primary, string secondary, out SafeUint reserve0, out SafeUint reserve1, out SafeUint totalSupply)
		{
			MySqlCommand mySqlCommand = sql.GetCommand("SELECT Reserve0, Reserve1, TotalSupply FROM UniswapReserves WHERE Pri = @pri AND Sec = @sec FOR UPDATE;");
			mySqlCommand.Parameters.AddWithValue("@pri", primary);
			mySqlCommand.Parameters.AddWithValue("@sec", secondary);
			using(MySqlDataReader mySqlDataReader = mySqlCommand.ExecuteReader()){
				if (mySqlDataReader.Read())
				{
					reserve0 = GetSafeUint(mySqlDataReader.GetString("Reserve0"));
					reserve1 = GetSafeUint(mySqlDataReader.GetString("Reserve1"));
					totalSupply = GetSafeUint(mySqlDataReader.GetString("TotalSupply"));
					CheckSafety2(mySqlDataReader.Read(), "Uniswap.NET: Duplicate LP records (should not reach here)!", true);
					return false;
				}
				else
				{
					reserve0 = zero;
					reserve1 = zero;
					totalSupply = zero;
					return true;
				}
			}
			
			
		}

		private static void WriteLP(SQLCommandFactory sql, string primary, string secondary, LPReserve lpreserve)
		{
			StringBuilder stringBuilder;
			if(lpreserve.insert){
				stringBuilder = new StringBuilder("INSERT INTO UniswapReserves (Reserve0, Reserve1, TotalSupply, Pri, Sec) VALUES (\"");
				stringBuilder.Append(lpreserve.reserve0.ToString() + "\", \"");
				stringBuilder.Append(lpreserve.reserve1.ToString() + "\", \"");
				stringBuilder.Append(lpreserve.totalSupply.ToString() + "\", @pri, @sec);");
			} else{
				stringBuilder = new StringBuilder("UPDATE UniswapReserves SET Reserve0 = \"");
				stringBuilder.Append(lpreserve.reserve0.ToString());
				stringBuilder.Append("\", Reserve1 = \"");
				stringBuilder.Append(lpreserve.reserve1.ToString());
				stringBuilder.Append("\", TotalSupply = \"");
				stringBuilder.Append(lpreserve.totalSupply.ToString());
				stringBuilder.Append("\" WHERE Pri = @pri AND Sec = @sec;");
			}
			MySqlCommand mySqlCommand = sql.GetCommand(stringBuilder.ToString());
			mySqlCommand.Parameters.AddWithValue("@pri", primary);
			mySqlCommand.Parameters.AddWithValue("@sec", secondary);
			mySqlCommand.Prepare();
			mySqlCommand.SafeExecuteNonQuery();
		}

		/// <summary>
		/// Mints Uniswap.NET LP tokens to trader
		/// </summary>
		public static LPReserve MintLP(this SQLCommandFactory sql, string pri, string sec, SafeUint amount0, SafeUint amount1, ulong to, LPReserve lpreserve)
		{
			sql.Debit(pri, to, amount0, true);
			sql.Debit(sec, to, amount1, true);

			bool insert = ReadLP(sql, pri, sec, out SafeUint reserve0, out SafeUint reserve1, out SafeUint totalSupply);
			string name = "LP_" + pri.Replace("_", "__") + "_" + sec;

			SafeUint liquidity;
			if (totalSupply.isZero)
			{
				liquidity = amount0.Mul(amount1).Sqrt().Sub(thousand, "Uniswap.NET: Insufficent liquidity minted!", false);
			}
			else
			{
				liquidity = amount0.Mul(totalSupply).Div(reserve0).Min(amount1.Mul(totalSupply).Div(reserve1));
			}
			CheckSafety2(liquidity.isZero, "Uniswap.NET: Insufficent liquidity minted!");
			lpreserve = new LPReserve(reserve0.Add(amount0), reserve1.Add(amount1), totalSupply.Add(liquidity), insert);
			WriteLP(sql, pri, sec, lpreserve);

			sql.Credit(name, to, liquidity, false);
			return lpreserve;
		}

		/// <summary>
		/// Burns Uniswap.NET LP tokens from trader
		/// </summary>
		public static LPReserve BurnLP(this SQLCommandFactory sql, string pri, string sec, SafeUint amount, ulong to, LPReserve lpreserve)
		{
			CheckSafety2(lpreserve.insert, "Uniswap.NET: Burn from empty pool!");
			string name = "LP_" + pri.Replace("_", "__") + "_" + sec;
			sql.Debit(name, to, amount, false);
			SafeUint remainingTotalSupply = lpreserve.totalSupply.Sub(amount, "Uniswap.NET: Burn exceeds total supply (should not reach here)!", true);

			SafeUint out0 = lpreserve.reserve0.Mul(amount).Div(lpreserve.totalSupply);
			SafeUint out1 = lpreserve.reserve1.Mul(amount).Div(lpreserve.totalSupply);
			CheckSafety2(out0.isZero || out1.isZero, "Uniswap.NET: Insufficent liquidity burned!");
			lpreserve = new LPReserve(lpreserve.reserve0.Sub(out0), lpreserve.reserve1.Sub(out1), remainingTotalSupply, false);
			WriteLP(sql, pri, sec, lpreserve);

			sql.Credit(pri, to, out0, true);
			sql.Credit(sec, to, out1, true);
			return lpreserve;
		}

		/// <summary>
		/// Burns Uniswap.NET LP tokens from trader
		/// </summary>
		public static LPReserve BurnLP(this SQLCommandFactory sql, string pri, string sec, SafeUint amount, ulong to)
		{
			return sql.BurnLP(pri, sec, amount, to, new LPReserve(sql, pri, sec));
		}

		
		/// <summary>
		/// Swaps tokens using Uniswap.NET (NO MUTATE)
		/// </summary>
		public static LPReserve SwapLP(this SQLCommandFactory sql, string pri, string sec, ulong userid, SafeUint input, bool buy, LPReserve lpreserve, out SafeUint output){
			CheckSafety2(input.isZero, "Uniswap.NET: Insufficent input amount!");
			CheckSafety2(lpreserve.insert, "Uniswap.NET: Pool does not exist!");
			SafeUint reserveIn;
			SafeUint reserveOut;
			string out_token;
			if(buy){
				out_token = sec;
				reserveIn = lpreserve.reserve0;
				reserveOut = lpreserve.reserve1;
			} else{
				out_token = pri;
				reserveIn = lpreserve.reserve1;
				reserveOut = lpreserve.reserve0;
			}
			CheckSafety2(reserveIn.isZero || reserveOut.isZero, "Uniswap.NET: Insufficent liquidity!");

			SafeUint amountInWithFee = input.Mul(afterfees);
			SafeUint numerator = amountInWithFee.Mul(reserveOut);
			SafeUint denominator = reserveIn.Mul(thousand).Add(amountInWithFee);
			output = numerator.Div(denominator);
			CheckSafety2(output.isZero, "Uniswap.NET: Insufficent output amount!");
			sql.Credit(out_token, userid, output, true);

			if (buy)
			{
				lpreserve = new LPReserve(lpreserve.reserve0.Add(input), lpreserve.reserve1.Sub(output), lpreserve.totalSupply, false);
			}
			else
			{
				lpreserve = new LPReserve(lpreserve.reserve0.Sub(output), lpreserve.reserve1.Add(input), lpreserve.totalSupply, false);
			}
			return lpreserve;
		}

		public static SafeUint ComputeProfitMaximizingTrade(SafeUint truePriceTokenB, LPReserve lpreserve, out bool buy)
		{
			SafeUint invariant = lpreserve.reserve0.Mul(lpreserve.reserve1);
			if(invariant.isZero){
				buy = false;
				return zero;
			}
			else{
				buy = lpreserve.reserve0.Mul(truePriceTokenB).Div(lpreserve.reserve1) < ether;

				SafeUint truePriceTokenIn;
				SafeUint truePriceTokenOut;
				if (buy)
				{
					truePriceTokenIn = ether;
					truePriceTokenOut = truePriceTokenB;
				}
				else
				{
					truePriceTokenIn = truePriceTokenB;
					truePriceTokenOut = ether;
				}

				SafeUint leftSide = invariant.Mul(thousand).Mul(truePriceTokenIn).Mul(truePriceTokenOut.Mul(afterfees)).Sqrt();
				SafeUint rightSide = truePriceTokenIn.Mul(thousand).Div(afterfees);
				if (leftSide < rightSide)
				{
					return zero;
				}
				else
				{
					return leftSide.Sub(rightSide);
				}
			}
		}
		/// <summary>
		/// Fills order using Uniswap.NET (NO MUTATE)
		/// </summary>
		private static LPReserve TryArb(this SQLCommandFactory sqlCommandFactory, string primary, string secondary, bool buy, Order instance, SafeUint price, LPReserve lpreserve)
		{
			if(lpreserve.reserve0.isZero || lpreserve.reserve1.isZero){
				return lpreserve;
			}
			SafeUint ArbitrageIn = ComputeProfitMaximizingTrade(price, lpreserve, out bool arbitrageBuy);


			if(arbitrageBuy == buy){
				ArbitrageIn = ArbitrageIn.Min(instance.Balance);
			} else{
				return lpreserve;
			}
			

			if (ArbitrageIn.isZero)
			{
				return lpreserve;
			} else{
				//Partial order cancellation
				instance.Debit2(ArbitrageIn);

				//Swap using Uniswap.NET
				return sqlCommandFactory.SwapLP(primary, secondary, instance.placedby, ArbitrageIn, buy, lpreserve, out _);
			}
		}
	}
}