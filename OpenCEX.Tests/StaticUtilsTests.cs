using Microsoft.VisualStudio.TestTools.UnitTesting;
using static jessielesbian.OpenCEX.StaticUtils;
using System;
using System.Collections.Generic;
using System.Text;
using jessielesbian.OpenCEX.SafeMath;

namespace jessielesbian.OpenCEX.Tests
{
	[TestClass()]
	public sealed class StaticUtilsTests
	{
		[TestMethod()]
		public void MatchOrdersTest()
		{
			ReducedInitSelector.set = true;
#pragma warning disable IDE0059 // Unnecessary assignment of a value
			SafeUint expandedHalf = GetSafeUint("500000000000000000");
#pragma warning restore IDE0059 // Unnecessary assignment of a value
			SafeUint expandedOne = GetSafeUint("1000000000000000000");
			SafeUint expandedTwo = GetSafeUint("2000000000000000000");
			SafeUint expanddedThree = GetSafeUint("3000000000000000000");
			Order buyorder = new Order(expandedTwo, expandedOne, expandedTwo, zero, 0, 0);
			Order sellorder = new Order(expanddedThree, expandedOne, expandedOne, zero, 0, 0);
			Assert.IsFalse(MatchOrders(buyorder, sellorder, true));
			sellorder = new Order(expandedOne, expandedOne, expandedOne, zero, 0, 0);
			Assert.IsTrue(MatchOrders(buyorder, sellorder, true));
			Assert.IsTrue(buyorder.Balance == expandedOne);
			Assert.IsTrue(sellorder.Balance == zero);
		}
	}
}