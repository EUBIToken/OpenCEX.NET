using jessielesbian.OpenCEX;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;

namespace jessielesbian.OpenCEX.RequestManager
{
	public class RequestAppender
	{
		private readonly HttpListener httpListener;

		public RequestAppender(HttpListener httpListener)
		{
			this.httpListener = httpListener ?? throw new ArgumentNullException(nameof(httpListener));
		}

		public void Loop()
		{
			while (true)
			{
				HttpListenerContext httpListenerContext = null;
				try
				{
					//Establish connection
					httpListenerContext = httpListener.GetContext();
				} catch{
					
				}
				
				if(httpListenerContext != null){
					StaticUtils.HandleHTTPRequest(httpListenerContext);
				}
			}
		}
	}
	public sealed class Request
	{
		public readonly RequestMethod method;
		public readonly int userID;
		public readonly IDictionary<string, object> args;

		public Request(RequestMethod method, int userID, IDictionary<string, object> args)
		{
			this.method = method ?? throw new ArgumentNullException(nameof(method));
			this.userID = userID;
			this.args = args ?? throw new ArgumentNullException(nameof(args));
		}
	}

	public abstract class RequestMethod{
		public abstract void Execute(SQLCommandFactory sqlCommandFactory, int userID, IDictionary<string, object> objects);
	}

	public sealed class RequestManager : IDisposable
	{
		private readonly SQLCommandFactory sqlCommandFactory = StaticUtils.GetSQL();
		private bool disposedValue;

		public void ExecuteRequest(Request request, int userID, bool keep)
		{
			StaticUtils.CheckSafety2(disposedValue, "Request manager disposed!");
			StaticUtils.CheckSafety(request, "Request can't be null!");

			try{
				request.method.Execute(sqlCommandFactory, userID, request.args);
			} catch(Exception e){
				Dispose();
				throw e;
			}

			if(keep){
				sqlCommandFactory.FlushTransaction(true);
			} else{
				Dispose();
			}
		}


		public void Dispose()
		{
			if (!disposedValue)
			{
				sqlCommandFactory.Dispose();
				disposedValue = true;
			}
		}
	}
}