using jessielesbian.OpenCEX.SafeMath;
using System.Numerics;
using System.Collections;
using System.Linq;
using System;

namespace jessielesbian.OpenCEX
{
	public static partial class StaticUtils{
		public static readonly SafeUint ether = GetSafeUint("1000000000000000000");
		public static readonly BigInteger decParseLimit = new BigInteger(1000000000000000000UL);
		public static readonly BigInteger ten = new BigInteger(10);
		public static SafeUint GetSafeUint(string number){
			number = number.ToLower();
			BigInteger bigInteger = new BigInteger(0);
			if(number.StartsWith("0x")){
				throw new SafetyException("Hexadecimal SafeUint not implemented yet!");
			} else{
				CheckSafety2(number == "", "SafeMath: Invaid Number!");
				BigInteger divisor = BigInteger.One;
				uint limit = (uint)(number.Length / 18);
				for(uint i = 0; i < limit; i++){
					divisor *= decParseLimit;
				}
				uint div2 = 1;
				limit = 18U - (uint)(number.Length % 18);
				if(limit < 18){
					for (uint i = 0; i < limit; i++)
					{
						div2 *= 10;
					}
				}
				

				while (number != ""){
					string chunk;
					bool nobrk = number.Length > 17;
					if (nobrk)
					{
						chunk = number.Substring(0, 18);
					} else{
						CheckSafety(divisor.IsOne, "SafeMath: Unreachable parse error!");
						bigInteger /= div2;
						chunk = number;
					}

					ulong preconv = Convert.ToUInt64(chunk);
					CheckSafety(preconv.ToString() == chunk, "Corrupted integer value!");
					bigInteger += new BigInteger(preconv) * divisor;
					divisor /= decParseLimit;
					if (nobrk)
					{
						number = number[18..];
					} else{
						break;
					}
					
				}
				return new SafeUint(bigInteger);
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