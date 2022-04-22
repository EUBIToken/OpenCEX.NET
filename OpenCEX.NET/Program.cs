using System;
using System.Threading;

namespace jessielesbian.OpenCEX
{
	public static class Program
	{
		[MTAThread]
		public static void Main()
		{
			Console.WriteLine("OpenCEX.NET: advanced-technology, open-source cryptocurrency exchange");
			Console.WriteLine("Made by Jessie Lesbian <jessielesbian@protonmail.com>");
			Console.WriteLine();
			Thread main = new Thread(StaticUtils.Start)
			{
				Name = "OpenCEX.NET main thread"
			};
			main.Start();
		}
	}
}
