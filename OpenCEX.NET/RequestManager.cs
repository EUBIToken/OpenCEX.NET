using jessielesbian.OpenCEX;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using jessielesbian.OpenCEX.RequestManager;
using jessielesbian.OpenCEX.SafeMath;

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
			object returns;
			try
			{
				returns = method.Execute(this);
			} catch(Exception e){
				sqlCommandFactory.DestroyTransaction(false, true);
				throw e;
			}
			
			if (method.needSQL)
			{
				sqlCommandFactory.DestroyTransaction(true, true);
			}
			return returns;

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
	public static partial class StaticUtils{
		public sealed class TestShitcoins : RequestMethod
		{
			private TestShitcoins(){

			}

			public static readonly RequestMethod instance = new TestShitcoins();

			public override object Execute(Request request)
			{
				ulong userId = request.GetUserID();
				request.Credit("shitcoin", userId, ether);
				request.Credit("scamcoin", userId, ether);
				return null;
			}

			protected override bool NeedSQL()
			{
				return true;
			}
		}
	}
}