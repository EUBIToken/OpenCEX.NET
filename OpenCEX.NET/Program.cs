using System;

namespace jessielesbian.OpenCEX
{
	public static class Program
	{
		public static void Main(string[] args)
		{
			Console.WriteLine("OpenCEX.NET: advanced-technology, open-source cryptocurrency exchange");
			Console.WriteLine("Made by Jessie Lesbian <jessielesbian@protonmail.com>");
			Console.WriteLine(StaticUtils.getSafeUint("123456789012345678901234567890"));
			Console.WriteLine();
			StaticUtils.Start();
		}
	}
}
