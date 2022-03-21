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
				httpListener.GetContext().Response.Close();
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
		
	}

	public abstract class RequestManager
	{
		protected static IDictionary<string, RequestMethod> RequestMethods = new Dictionary<string, RequestMethod>();
	}
}