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
		public static readonly BigInteger ten = new BigInteger(10);
		public static readonly SafeUint ether = GetSafeUint("1000000000000000000");
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

		public override string ToString()
		{
			return bigInteger.ToString();
		}
	}
}