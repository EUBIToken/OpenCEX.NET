using jessielesbian.OpenCEX.SafeMath;
using System.Numerics;
using System.Collections;
using System.Linq;
using System;
using System.Globalization;

namespace jessielesbian.OpenCEX
{
	public static partial class StaticUtils{
		public static readonly BigInteger decParseLimit = new BigInteger(1000000000000000000UL);
		public static readonly SafeUint ether = GetSafeUint("1000000000000000000");
		public static readonly SafeUint zero = new SafeUint(BigInteger.Zero);
		public static readonly SafeUint one = new SafeUint(BigInteger.One);
		public static SafeUint GetSafeUint(string number){
			if (number.StartsWith("0x"))
			{
				return new SafeUint(BigInteger.Parse(number.Substring(2), NumberStyles.AllowHexSpecifier));
			}
			else{
				return new SafeUint(BigInteger.Parse(number, NumberStyles.None));
			}
		}
	}
}

namespace jessielesbian.OpenCEX.SafeMath{
	public sealed class SafeUint{
		private readonly BigInteger bigInteger;
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
			return new SafeUint(bigInteger + other.bigInteger);
		}

		public SafeUint Sub(SafeUint other, string msg = "SafeMath: Subtraction Overflow!")
		{
			return new SafeUint(bigInteger - other.bigInteger, msg);
		}

		public SafeUint Mul(SafeUint other)
		{
			return new SafeUint(bigInteger * other.bigInteger);
		}

		public SafeUint Div(SafeUint other, string msg = "SafeMath: Divide by zero!")
		{
			StaticUtils.CheckSafety(other.isZero, msg);
			return new SafeUint(bigInteger / other.bigInteger, msg);
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
			return x.bigInteger == y.bigInteger;
		}

		public static bool operator !=(SafeUint x, SafeUint y)
		{
			return x.bigInteger != y.bigInteger;
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
	}
}