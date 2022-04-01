using jessielesbian.OpenCEX.SafeMath;
using System.Numerics;
using System.Collections;
using System.Linq;
using System;
using System.Globalization;
using Nethereum.Hex.HexTypes;

namespace jessielesbian.OpenCEX
{
	public static partial class StaticUtils{
		public static readonly SafeUint day = new SafeUint(new BigInteger(86400));
		public static readonly SafeUint basegas = new SafeUint(new BigInteger(21000));
		public static readonly SafeUint basegas2 = new SafeUint(new BigInteger(1000000));
		public static readonly SafeUint e16 = new SafeUint(new BigInteger(65536));
		public static readonly SafeUint gwei = new SafeUint(new BigInteger(990000000));
		public static readonly SafeUint ten = new SafeUint(new BigInteger(10));
		public static readonly SafeUint ether = GetSafeUint("1000000000000000000");
		public static readonly SafeUint zero = new SafeUint(BigInteger.Zero);
		public static readonly SafeUint one = new SafeUint(BigInteger.One);
		public static readonly SafeUint two = new SafeUint(2);
		public static readonly SafeUint three = new SafeUint(3);
		public static readonly SafeUint thousand = new SafeUint(1000);
		public static readonly SafeUint afterfees = new SafeUint(997);

		public static SafeUint GetSafeUint(string number){
			if (number.StartsWith("0x"))
			{
				number = number.Substring(2);
				if (number[0] != '0'){
					number = '0' + number;
				}
				if(number.Length % 2 == 1){
					number = '0' + number;
				}
				return new SafeUint(BigInteger.Parse(number, NumberStyles.AllowHexSpecifier));
			}
			else{
				return new SafeUint(BigInteger.Parse(number, NumberStyles.None));
			}
		}

		public static string SafeSerializeSafeUint(SafeUint stuff){
			if(stuff is null){
				return null;
			} else{
				return stuff.ToString();
			}
		}
		public static decimal GetAmount2(this SafeUint amount)
		{
			if (amount < ether)
			{
				return Convert.ToDecimal("0." + amount.ToString().PadLeft(18, '0'));
			}
			else
			{
				return Convert.ToDecimal(amount.Div(ether) + "." + amount.Mod(ether));
			}
		}
	}
}

namespace jessielesbian.OpenCEX.SafeMath{
	public sealed class SafeUint{
		public readonly BigInteger bigInteger;
		public readonly bool isZero;
		public readonly bool isOne;

		public SafeUint(BigInteger bigInteger, string msg = "SafeMath: Negative unsigned big integer!")
		{
			StaticUtils.CheckSafety2(bigInteger.Sign < 0, msg);
			this.bigInteger = bigInteger;
			isZero = bigInteger.IsZero;
			isOne = bigInteger.IsOne;
		}

		public SafeUint Add(SafeUint other){
			return new SafeUint(bigInteger + other.bigInteger, "SafeMath: Unreachable Add Error (should not reach here)!");
		}

		public SafeUint Sub(SafeUint other, string msg = "SafeMath: Subtraction Overflow!")
		{
			return new SafeUint(bigInteger - other.bigInteger, msg);
		}

		public SafeUint Mul(SafeUint other)
		{
			return new SafeUint(bigInteger * other.bigInteger, "SafeMath: Unreachable Multiply Error (should not reach here)!");
		}

		public SafeUint Div(SafeUint other, string msg = "SafeMath: Divide by zero!")
		{
			StaticUtils.CheckSafety2(other.isZero, msg);
			BigInteger b = bigInteger / other.bigInteger;
			return new SafeUint(b, "SafeMath: Unreachable Divide Error (should not reach here)!");
		}

		public SafeUint Mod(SafeUint other, string msg = "SafeMath: Modulo by zero!")
		{
			StaticUtils.CheckSafety2(other.isZero, msg);
			BigInteger b = bigInteger % other.bigInteger;
			return new SafeUint(b, "SafeMath: Unreachable Modulo Error (should not reach here)!");
		}

		public SafeUint Max(SafeUint other){
			if(bigInteger > other.bigInteger){
				return this;
			} else{
				return other;
			}
		}

		public SafeUint Min(SafeUint other)
		{
			if (bigInteger < other.bigInteger)
			{
				return this;
			}
			else
			{
				return other;
			}
		}

		public static bool operator >(SafeUint x, SafeUint y)
		{
			return x.bigInteger > y.bigInteger;
		}

		public static bool operator <(SafeUint x, SafeUint y)
		{
			return x.bigInteger < y.bigInteger;
		}

		public static bool operator ==(SafeUint x, SafeUint y)
		{
			if(x is null && y is null){
				return true;
			} else if(x is null){
				return false;
			} else if(y is null){
				return false;
			} else{
				return x.bigInteger == y.bigInteger;
			}
			
		}

		public static bool operator !=(SafeUint x, SafeUint y)
		{
			return !(x == y);
		}

		public override bool Equals(object obj)
		{
			if(obj is SafeUint safeUint){
				return bigInteger == safeUint.bigInteger;
			} else{
				return false;
			}
		}

		public override string ToString()
		{
			return bigInteger.ToString();
		}

		public override int GetHashCode()
		{
			return bigInteger.GetHashCode();
		}

		public string ToHex(bool prefix = true, bool pad256 = true){
			string postfix = new HexBigInteger(bigInteger).HexValue.ToLower().Substring(2);
			if(pad256)
			{
				StaticUtils.CheckSafety2(postfix.Length > 64, "256-bit integer overflow!");
				postfix = postfix.PadLeft(64, '0');
			} else{
				while(postfix.StartsWith('0')){
					postfix = postfix.Substring(1);
				}
				if (postfix == string.Empty){
					postfix = "0";
				}
			}
			
			if (prefix){
				return "0x" + postfix;
			} else{
				return postfix;
			}
			
		}
	}
}