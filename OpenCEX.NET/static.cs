using System;
using System.Collections.Generic;
using System.Net;
using Newtonsoft.Json;
using System.Runtime.Serialization;
using System.Threading;
using jessielesbian.OpenCEX.RequestManager;
using MySql.Data.MySqlClient;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using System.Text;
using Newtonsoft.Json.Linq;
using System.Web;

namespace jessielesbian.OpenCEX{
	public sealed class SafetyException : Exception
	{
		public SafetyException()
		{
		}

		public SafetyException(string message) : base(message)
		{
		}

		public SafetyException(SerializationInfo info, StreamingContext context) : base(info, context)
		{
		}

		public SafetyException(string message, Exception innerException) : base(message, innerException)
		{
		}
	}

	public sealed class SQLCommandFactory : IDisposable{
		private readonly MySqlConnection mySqlConnection;
		private MySqlTransaction mySqlTransaction;
		private bool disposedValue;

		public SQLCommandFactory(MySqlConnection mySqlConnection, MySqlTransaction mySqlTransaction)
		{
			this.mySqlConnection = mySqlConnection ?? throw new ArgumentNullException(nameof(mySqlConnection));
			this.mySqlTransaction = mySqlTransaction ?? throw new ArgumentNullException(nameof(mySqlTransaction));
		}
		
		private void RequireTransaction(){
			StaticUtils.CheckSafety2(disposedValue, "MySQL connection already disposed!");
			StaticUtils.CheckSafety(mySqlTransaction, "MySQL transaction not open!");
		}

		public MySqlCommand GetCommand(string cmd){
			RequireTransaction();
			return new MySqlCommand(cmd, mySqlConnection, mySqlTransaction);
		}

		public void FlushTransaction(bool commit){
			RequireTransaction();
			try
			{
				if (commit)
				{
					mySqlTransaction.Commit();
				}
				else
				{
					mySqlTransaction.Rollback();
				}

				mySqlTransaction.Dispose();
				mySqlTransaction = mySqlConnection.BeginTransaction();
			}
			catch
			{
				throw new SafetyException("Unable to flush MySQL transaction!");
			}
			StaticUtils.CheckSafety(mySqlConnection, "Unable to flush MySQL transaction!");
		}

		public void DestroyTransaction(bool commit){
			RequireTransaction();
			try
			{
				if(commit){
					mySqlTransaction.Commit();
				} else{
					mySqlTransaction.Rollback();
				}
				
				mySqlTransaction.Dispose();
				mySqlTransaction = null;
			} catch{
				throw new SafetyException("Unable to commit MySQL transaction!");
			}	
		}

		public void BeginTransaction(){
			StaticUtils.CheckSafety2(disposedValue, "MySQL connection already disposed!");
			StaticUtils.CheckSafety2(mySqlTransaction, "MySQL transaction already exist!");
			mySqlTransaction = mySqlConnection.BeginTransaction();
		}

		public void Dispose()
		{
			GC.SuppressFinalize(this);
			if (!disposedValue)
			{
				if(mySqlTransaction != null){
					DestroyTransaction(false);
				}
				mySqlConnection.Close();

				// TODO: free unmanaged resources (unmanaged objects) and override finalizer
				// TODO: set large fields to null
				disposedValue = true;
			}
		}

		~SQLCommandFactory()
		{
			Dispose();
		}
	}

	public static class StaticUtils{
		private static readonly System.Collections.IDictionary config = Environment.GetEnvironmentVariables();
		public static void CheckSafety(bool status, string message = "An unknown error have occoured!"){
			if(!status){
				throw new SafetyException(message);
			}
		}

		public static void CheckSafety2(bool status, string message = "An unknown error have occoured!")
		{
			if (status)
			{
				throw new SafetyException(message);
			}
		}

		public static void CheckSafety(object obj, string message = "An unknown error have occoured!")
		{
			if (obj == null)
			{
				throw new SafetyException(message);
			}
		}

		public static void CheckSafety2(object obj, string message = "An unknown error have occoured!")
		{
			if (obj != null)
			{
				throw new SafetyException(message);
			}
		}

		public static string GetEnv(string temp){
			if (temp != "PORT")
			{
				temp = "OpenCEX_" + temp;
			}
			try{
				temp = (string)config[temp];
			} catch{
				throw new SafetyException("Unable to cast enviroment variable to string!");
			}
			
			CheckSafety(temp, "Unknown enviroment variable!");
			return temp;
		}

		

		public static SQLCommandFactory GetSQL(){
			MySqlConnection mySqlConnection = null;
			try
			{
				mySqlConnection = new MySqlConnection(GetEnv("SQLConnectionString"));
				MySqlTransaction tx = mySqlConnection.BeginTransaction();

				
				CheckSafety(tx, "MySQL connection establishment failed: invalid MySQL transaction object!");
				
				return new SQLCommandFactory(mySqlConnection, tx);
			}
			catch (SafetyException e)
			{
				//This is safe, since the only way we can get here is
				//a failure in the transaction creation process
				mySqlConnection.Close();
				throw e;
			}
			catch
			{
				if(mySqlConnection != null){
					mySqlConnection.Close();
				}
				throw new SafetyException("MySQL connection establishment failed!");
			}
		}
		private static readonly HttpListener httpListener = new HttpListener();
		private static readonly ManualResetEventSlim terminateMainThread = new ManualResetEventSlim();
		private static bool dispose = true;
		private static readonly JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings();
		public static readonly Dictionary<string, RequestMethod> requestMethods = new Dictionary<string, RequestMethod>();
		public static readonly string underlying = GetEnv("Underlying");
		static StaticUtils(){
			jsonSerializerSettings.MaxDepth = 3;
			jsonSerializerSettings.MissingMemberHandling = MissingMemberHandling.Error;

			//Redirected Request Methods
			string[] redirectedRequestMethods = {"get_chart", "bid_ask"};
			foreach(string meth in redirectedRequestMethods){
				requestMethods.Add(meth, new RedirectedRequestMethod(meth));
			}
		}

		private sealed class RedirectedRequestMethod : RequestMethod
		{
			private readonly string name;

			public RedirectedRequestMethod(string name)
			{
				this.name = name ?? throw new ArgumentNullException(nameof(name));
			}

			public override object Execute(SQLCommandFactory sqlCommandFactory, HttpListenerContext httpListenerContext, IDictionary<string, object> objects)
			{
				WebRequest httpWebRequest = WebRequest.Create(underlying);
				httpWebRequest.Method = "POST";
				string cookieHeader = httpListenerContext.Request.Headers.Get("Cookie");
				if(cookieHeader != null){
					httpWebRequest.Headers.Add("Cookie", cookieHeader);
				}
				UnprocessedRequest unprocessedRequest = new UnprocessedRequest();
				unprocessedRequest.method = name;
				unprocessedRequest.data = objects;
				httpWebRequest.ContentType = "application/x-www-form-urlencoded";
				httpWebRequest.Headers.Add("Origin", origin);
				byte[] data = Encoding.ASCII.GetBytes(JsonConvert.SerializeObject(unprocessedRequest));
				httpWebRequest.ContentLength = data.Length;
				using (var stream = httpWebRequest.GetRequestStream())
				{
					stream.Write(data, 0, data.Length);
				}
				WebResponse webResponse = httpWebRequest.GetResponse();
				string returns = new StreamReader(webResponse.GetResponseStream()).ReadToEnd();
				webResponse.Close();
				JObject obj = JObject.Parse(returns);
				JToken token = null;
				CheckSafety(obj.TryGetValue("status", out token), "Missing request status!");
				if(token.ToObject<string>() == "success")
				{
					CheckSafety(obj.TryGetValue("returns", out token), "Missing request returns!");
					token = token.First;
					CheckSafety(token, "Missing request returns!");
					return token.ToObject<object>();
				} else{
					CheckSafety(obj.TryGetValue("reason", out token), "Missing error reason!");
					token = token.First;
					CheckSafety(token, "Missing error reason!");
					throw new SafetyException(token.ToObject<string>());
				}
			}
		}

		public static void Start(){
			AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;
			ushort port = Convert.ToUInt16(GetEnv("PORT"));
			lock (httpListener){
				httpListener.Prefixes.Add($"http://*:{port}/");
				httpListener.Start();
			}

			ushort thrlimit = Convert.ToUInt16(GetEnv("RequestAppenderThreadCount"));
			for(ushort i = 0; i < thrlimit; i++){
				Thread thread = new Thread(new RequestAppender(httpListener).Loop);
				thread.IsBackground = true;
				thread.Name = "OpenCEX.NET Request Appender Thread";
				thread.Start();
			}
			terminateMainThread.Wait();
			terminateMainThread.Dispose();
		}

		private static void CurrentDomain_ProcessExit(object sender, EventArgs e)
		{
			lock (httpListener)
			{
				//Stop listening
				if(httpListener.IsListening)
				{
					httpListener.Stop();
				}

				//Dispose HTTP listener
				if (dispose)
				{
					httpListener.Close();
					dispose = false;
				}
			}

			lock(terminateMainThread){
				//Terminate main thread
				if (!terminateMainThread.IsSet)
				{
					terminateMainThread.Set();
				}
			}
		}
		[JsonObject(MemberSerialization.Fields)]
		private sealed class FailedRequest{
			private readonly string status = "error";
			private readonly string reason;

			public FailedRequest(string reason)
			{
				this.reason = reason ?? throw new ArgumentNullException(nameof(reason));
			}
		}
		public static readonly bool debug = Convert.ToBoolean(GetEnv("Debug"));

		[JsonObject(MemberSerialization.Fields)]
		private sealed class UnprocessedRequest{
			public string method;
			public IDictionary<string, object> data;
		}

		private static readonly string origin = GetEnv("Origin");

		public static void HandleHTTPRequest(HttpListenerContext httpListenerContext){
			
			try{
				HttpListenerRequest httpListenerRequest = httpListenerContext.Request;
				HttpListenerResponse httpListenerResponse = httpListenerContext.Response;
				StreamWriter streamWriter = new StreamWriter(httpListenerResponse.OutputStream, httpListenerResponse.ContentEncoding);
				try
				{
					//Headers
					httpListenerResponse.AddHeader("Access-Control-Allow-Origin", origin);
					httpListenerResponse.AddHeader("Access-Control-Allow-Credentials", "true");
					httpListenerResponse.AddHeader("Strict-Transport-Security", "max-age=63072000");

					//POST requests only
					CheckSafety(httpListenerRequest.HttpMethod == "POST", "Illegal request method!");

					//CSRF protection
					CheckSafety(httpListenerRequest.Headers.Get("Origin") == origin, "Illegal origin!");


					//POST parameter
					StreamReader streamReader = new StreamReader(httpListenerRequest.InputStream, httpListenerRequest.ContentEncoding);
					string body = streamReader.ReadToEnd();

					CheckSafety(body.StartsWith("OpenCEX_request_body="), "Missing request body!");
					body = HttpUtility.UrlDecode(body.Substring(21));

					UnprocessedRequest[] unprocessedRequests;

					try
					{
						unprocessedRequests = JsonConvert.DeserializeObject<UnprocessedRequest[]>(body, jsonSerializerSettings);
					} catch{
						throw new SafetyException("Invalid request");
					}

					CheckSafety(unprocessedRequests.Length < 10, "Too many requests in batch!");

					LinkedList<Request> requests = new LinkedList<Request>();
					foreach(UnprocessedRequest unprocessedRequest in unprocessedRequests){
						CheckSafety(unprocessedRequest.method, "Missing request method!");
						RequestMethod requestMethod = null;
						CheckSafety(requestMethods.TryGetValue(unprocessedRequest.method, out requestMethod), "Unknown request method!");
						IDictionary<string, object> data = (unprocessedRequest.data == null) ? new Dictionary<string, object>(0) : unprocessedRequest.data;
						requests.AddLast(new Request(requestMethod, httpListenerContext, data));
					}
					
				}
				catch (ArgumentNullException e)
				{
					string error = debug ? e.ToString() : "Null argument passed to function!";
					streamWriter.Write(JsonConvert.SerializeObject(new FailedRequest(error)));
				}
				catch (SafetyException e){
					string error = debug ? e.ToString() : e.Message;
					streamWriter.Write(JsonConvert.SerializeObject(new FailedRequest(error)));
				}
				catch (Exception e){
					string error = debug ? ("Unexpected internal server error: " + e.ToString()) : "Unexpected internal server error!";
					streamWriter.Write(JsonConvert.SerializeObject(new FailedRequest(error)));
				}
				finally{
					streamWriter.Flush();
					httpListenerResponse.Close();
				}
			}
			
			catch
			{
				
			}
		}
	}
}