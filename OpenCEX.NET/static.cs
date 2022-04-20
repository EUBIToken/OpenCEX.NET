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
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

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

	public abstract class ConcurrentJob{
		public readonly PooledManualResetEvent sync = PooledManualResetEvent.GetInstance(false);
		public Exception exception = null;
		public object returns = null;
		public object Wait(){
			sync.Wait();
			sync.Dispose();
			if(exception == null){
				return returns;
			} else{
				if(exception is SafetyException){
					throw exception;
				} else{
					throw new SafetyException("Concurrent job failed!", exception);
				}
				
			}
		}

		public void Execute(){
			try{
				StaticUtils.CheckSafety2(sync.IsSet, "Job already executed!");
				returns = ExecuteIMPL();
			} catch(Exception e){
				exception = e;
			} finally{
				sync.Set();
			}
		}
		protected abstract object ExecuteIMPL();
	}

	public static partial class StaticUtils{
		public static void CheckSingletonResult(this MySqlDataReader mySqlDataReader){
			if(mySqlDataReader.NextResult()){
				mySqlDataReader.Close();
				throw new SafetyException("Unexpected trailing data (should not reach here)!");
			}
		}

		private static readonly System.Collections.IDictionary config = Environment.GetEnvironmentVariables();
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void CheckSafety(bool status, string message = "An unknown error have occoured!", bool critical = false){
			if(!status){
				if(critical){
					throw new Exception(message);
				} else{
					throw new SafetyException(message);
				}
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void CheckSafety2(bool status, string message = "An unknown error have occoured!", bool critical = false)
		{
			if (status)
			{
				if (critical)
				{
					throw new Exception(message);
				}
				else
				{
					throw new SafetyException(message);
				}
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void CheckSafety(object obj, string message = "An unknown error have occoured!", bool critical = false)
		{
			if (obj == null)
			{
				if (critical)
				{
					throw new Exception(message);
				}
				else
				{
					throw new SafetyException(message);
				}
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void CheckSafety2(object obj, string message = "An unknown error have occoured!", bool critical = false)
		{
			if (obj != null)
			{
				if (critical)
				{
					throw new Exception(message);
				}
				else
				{
					throw new SafetyException(message);
				}
			}
		}

		public static string GetEnv(string temp){
			if (!(temp == "PORT" || temp == "DYNO"))
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

		public static bool Multiserver = Convert.ToBoolean(GetEnv("Multiserver"));

		private static PooledManualResetEvent depositBlocker = PooledManualResetEvent.GetInstance(false);

		private static string SQLConnectionString = GetEnv("SQLConnectionString");

		public static SQLCommandFactory GetSQL(){
			MySqlConnection mySqlConnection = null;
			try
			{
				mySqlConnection = new MySqlConnection(SQLConnectionString);
				mySqlConnection.Open();
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
			catch (Exception e)
			{
				if(mySqlConnection != null){
					mySqlConnection.Close();
				}
				if(debug){
					throw e;
				} else{
					throw new SafetyException("MySQL connection establishment failed!");
				}
			}
		}

		private sealed class DummyConcurrentJob : ConcurrentJob
		{
			protected override object ExecuteIMPL()
			{
				return null;
			}
		}

		private static readonly HttpListener httpListener = new HttpListener();
		private static readonly JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings();
		public static readonly Dictionary<string, RequestMethod> requestMethods = new Dictionary<string, RequestMethod>();
		public static readonly int MaximumBalanceCacheSize = (int)(Convert.ToUInt32(GetEnv("MaximumBalanceCacheSize")) - 1);
		public static readonly ushort thrlimit = Convert.ToUInt16(GetEnv("ExecutionThreadCount"));

		//OpenCEX.NET Managed Thread
		private sealed class ManagedAbortThread{
			private readonly ManagedAbortThread inner;
			private static ManagedAbortThread instance = null;
			private readonly Thread monitor;
			private static readonly object locker = new object();

			private ManagedAbortThread(ManagedAbortThread inner, Thread monitor)
			{
				this.inner = inner;
				this.monitor = monitor ?? throw new ArgumentNullException(nameof(monitor));
			}

			public static void Append(Thread thread){
				lock(locker){
					instance = new ManagedAbortThread(instance, thread);
				}
			}
			private void Join(){
				if(inner != null){
					inner.Join();
				}
				monitor.Join();
			}
			public static void JoinAll(){
				lock (locker)
				{
					if (instance != null)
					{
						instance.Join();
					}
				}
			}
		}
		static StaticUtils(){
			jsonSerializerSettings.MaxDepth = 3;
			jsonSerializerSettings.MissingMemberHandling = MissingMemberHandling.Error;

			//Native request methods
			requestMethods.Add("get_test_tokens", TestShitcoins.instance);
			requestMethods.Add("cancel_order", CancelOrder.instance);
			requestMethods.Add("place_order", PlaceOrder.instance);
			requestMethods.Add("bid_ask", BidAsk.instance);
			requestMethods.Add("deposit", Deposit.instance);
			requestMethods.Add("balances", GetBalances.instance);
			requestMethods.Add("client_name", GetUsername.instance);
			requestMethods.Add("eth_deposit_address", GetEthDepAddr.instance);
			requestMethods.Add("login", Login.instance);
			requestMethods.Add("withdraw", Withdraw.instance);
			requestMethods.Add("create_account", CreateAccount.instance);
			requestMethods.Add("logout", Logout.instance);
			requestMethods.Add("load_active_orders", LoadActiveOrders.instance);
			requestMethods.Add("get_chart", GetChart.instance);
			requestMethods.Add("mint_lp", MintLP1.instance);

			//Start threads
			Thread thread;
			for (ushort i = 0; i < thrlimit;)
			{
				thread = new Thread(ExecutionThread);
				thread.Name = "OpenCEX.NET Execution Thread #" + (++i).ToString();
				thread.Start();
				ManagedAbortThread.Append(thread);
			}
			thread = new Thread(QOSWatchdog);
			thread.Name = "OpenCEX.NET Watchdog Thread";
			thread.Start();
			ManagedAbortThread.Append(thread);

			if (leadServer){
				//Start deposit manager
				thread = new Thread(DepositManager);
				thread.Name = "OpenCEX.NET deposit manager thread";
				thread.Start();
				ManagedAbortThread.Append(thread);

				//Start transaction sending manager
				thread = new Thread(SendingManagerThread.instance.DoStupidThings);
				thread.Name = "OpenCEX.NET transaction sending manager thread";
				thread.Start();
				ManagedAbortThread.Append(thread);
			}
		}

		public static T Await2<T>(Task<T> task){
			PooledManualResetEvent manualResetEventSlim = PooledManualResetEvent.GetInstance(false);
			task.GetAwaiter().OnCompleted(manualResetEventSlim.Set);
			manualResetEventSlim.Wait();
			manualResetEventSlim.Dispose();
			CheckSafety(task.IsCompleted, "Async task not completed (should not reach here)!", true);
			Exception e = task.Exception;
			if (e == null)
			{
				return task.Result;
			}
			else
			{
				throw new SafetyException("Async task request failed!", e);
			}
		}

		public static void Await2(Task task)
		{
			PooledManualResetEvent manualResetEventSlim = PooledManualResetEvent.GetInstance(false);
			task.GetAwaiter().OnCompleted(manualResetEventSlim.Set);
			manualResetEventSlim.Wait();
			manualResetEventSlim.Dispose();
			CheckSafety(task.IsCompleted, "Async task not completed (should not reach here)!", true);
			Exception e = task.Exception;
			if (e != null)
			{
				throw new SafetyException("Async task failed!", e);
			}
		}

		public static readonly string CookieOrigin = GetEnv("CookieOrigin");

		private static readonly ConcurrentQueue<ConcurrentJob> concurrentJobs = new ConcurrentQueue<ConcurrentJob>();

		private static readonly PooledManualResetEvent manualResetEventSlim = PooledManualResetEvent.GetInstance(false);
		private static void ExecutionThread(){
			//Critical section (abort not allowed)
			while (!abort)
			{
				if (concurrentJobs.TryDequeue(out ConcurrentJob concurrentJob))
				{
					concurrentJob.Execute();
				}
				else
				{
					manualResetEventSlim.Reset();
					manualResetEventSlim.Wait(100);
				}
			}
			Console.WriteLine(Thread.CurrentThread.Name + " stopped!");
		}

		private sealed class ProcessHTTP : ConcurrentJob
		{
			private readonly HttpListenerContext httpListenerContext;

			public ProcessHTTP(HttpListenerContext httpListenerContext)
			{
				this.httpListenerContext = httpListenerContext ?? throw new ArgumentNullException(nameof(httpListenerContext));
			}

			protected override object ExecuteIMPL()
			{
				HandleHTTPRequest(httpListenerContext);
				return null;
			}
		}

		private static volatile bool abort = false;

		public static void Append(ConcurrentJob[] concurrentJobs2)
		{
			foreach(ConcurrentJob concurrentJob in concurrentJobs2)
			{
				concurrentJobs.Enqueue(concurrentJob);
			}
			if ((!manualResetEventSlim.IsSet) && concurrentJobs.IsEmpty)
			{
				manualResetEventSlim.Set();
			}
		}

		public static void Append(ConcurrentJob concurrentJob){
			concurrentJobs.Enqueue(concurrentJob);
			manualResetEventSlim.Set();
		}

		public static void Start(){
			//Start HTTP listening
			AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;
			ushort port = Convert.ToUInt16(GetEnv("PORT"));
			lock (httpListener){
				httpListener.Prefixes.Add($"http://*:{port}/");
				httpListener.Start();
			}

			while (httpListener.IsListening)
			{
				HttpListenerContext httpListenerContext = null;
				try
				{
					//Establish connection
					httpListenerContext = httpListener.GetContext();
				}
				catch
				{
					
				}

				if(httpListenerContext == null)
				{
					continue;
				} else{
					Append(new ProcessHTTP(httpListenerContext));
				}
				
			}
			httpListener.Close();

			//Wait for all execution threads to complete
			while(!concurrentJobs.IsEmpty){
				ConcurrentJob concurrentJob = new DummyConcurrentJob();
				Append(concurrentJob);
				concurrentJob.Wait();
			}

			depositBlocker.Set();
		}

		private class WatchdogPing : ConcurrentJob
		{
			protected override object ExecuteIMPL()
			{
				if(Interlocked.Decrement(ref watchdogCounter) == MaximumWatchdogLag){
					Console.WriteLine("Watchdog: Server load normal, resume processing requests!");
				}
				
				return true;
			}
		}

		private static volatile int watchdogCounter = 0;
		private static readonly int MaximumWatchdogLag = Convert.ToInt32(GetEnv("MaximumWatchdogLag"));

		private static void QOSWatchdog(){
			while (!abort)
			{
				if ((Interlocked.Increment(ref watchdogCounter) - 1) == MaximumWatchdogLag)
				{
					Console.Error.WriteLine("Watchdog: Server overloaded, stop processing requests!");
				}
				Append(new WatchdogPing());
				Thread.Sleep(20);
			}
			Console.WriteLine(Thread.CurrentThread.Name + " stopped!");
		}

		private static void CurrentDomain_ProcessExit(object sender, EventArgs e)
		{
			lock (httpListener)
			{
				//Abort once
				if (abort)
				{
					return;
				}
				else
				{
					abort = true;
				}

				//Stop listening
				if (httpListener.IsListening)
				{
					httpListener.Stop();
				}
			}
			ManagedAbortThread.JoinAll();
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

		//The lead server is responsible for deposit finalization.
		public static bool leadServer = GetEnv("DYNO") == "web.1";

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

					CheckSafety2(watchdogCounter > MaximumWatchdogLag, "Server overloaded, please try again later!");

					UnprocessedRequest[] unprocessedRequests;

					try
					{
						unprocessedRequests = JsonConvert.DeserializeObject<UnprocessedRequest[]>(body, jsonSerializerSettings);
					} catch{
						throw new SafetyException("Invalid request");
					}

					CheckSafety(unprocessedRequests.Length < 10, "Too many requests in batch!");
					if(unprocessedRequests.Length == 0){
						streamWriter.Write("{\"status\": \"success\", \"returns\": []}");
						return;
					}

					Queue<Request> requests = new Queue<Request>();
					foreach(UnprocessedRequest unprocessedRequest in unprocessedRequests){
						CheckSafety(unprocessedRequest.method, "Missing request method!");
						RequestMethod requestMethod = null;
						CheckSafety(requestMethods.TryGetValue(unprocessedRequest.method, out requestMethod), "Unknown request method!");
						IDictionary<string, object> data = (unprocessedRequest.data == null) ? new Dictionary<string, object>(0) : unprocessedRequest.data;
						requests.Enqueue(new Request(requestMethod.needSQL ? GetSQL() : null, requestMethod, httpListenerContext, data));
					}

					Queue<Request> secondExecute = new Queue<Request>();
					Request request;
					while (requests.TryDequeue(out request))
					{
						concurrentJobs.Enqueue(request);
						secondExecute.Enqueue(request);
					}
					requests = secondExecute;
					secondExecute = null;
					manualResetEventSlim.Set();
					Queue<object> returns = new Queue<object>();
					while (requests.TryDequeue(out request))
					{
						returns.Enqueue(request.Wait());
						if(request.method is Deposit && !Multiserver){
							depositBlocker.Set();
						}
					}

					streamWriter.Write("{\"status\": \"success\", \"returns\": " + JsonConvert.SerializeObject(returns.ToArray()) + "}");
				}
				catch (ArgumentNullException e)
				{
					string error = debug ? e.ToString() : "Null argument passed to function!";
					streamWriter.Write(JsonConvert.SerializeObject(new FailedRequest(error)));
				}
				catch (SafetyException e){
					string error = debug ? e.ToString() : e.Message;
					if(!debug){
						Exception inner = e.InnerException;
						while (inner != null){
							if(inner is SafetyException){
								inner = inner.InnerException;
							} else{
								Console.Error.WriteLine("Unexpected internal server error: " + e.ToString());
								break;
							}
						}
					}
					streamWriter.Write(JsonConvert.SerializeObject(new FailedRequest(error)));
				}
				catch (Exception e){
					string error;
					if (debug){
						error = "Unexpected internal server error: " + e.ToString();
					} else{
						error = "Unexpected internal server error!";
						Console.Error.WriteLine("Unexpected internal server error: " + e.ToString());
					}
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

		/// <summary>
		/// Safe type checking for request args
		/// </summary>
		/// <returns></returns>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static T ExtractRequestArg<T>(this Request request, string key){
			CheckSafety(key, "Key should not be null (should not reach here)!", true);
			string postfix = key + '!';
			CheckSafety(request.args.TryGetValue(key, out object temp), "Missing request argument: " + postfix);
			try{
				return (T)temp;
			} catch{
				throw new SafetyException("Incorrect type for request argument: " + postfix);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static void ThrowInternal2(string reason){
			throw new SafetyException(reason, new Exception(reason));
		}

		public static readonly string[] listedTokensHint = new string[] { "shitcoin", "scamcoin", "CLICK", "MATIC", "MintME", "BNB", "PolyEUBI", "EUBI", "1000x", "Dai", "LP_MATIC_PolyEUBI", "LP_MintME_MATIC", "LP_MintME_BNB", "LP_MintME_PolyEUBI", "LP_MintME_EUBI", "LP_MintME_1000x", "LP_BNB_PolyEUBI", "LP_shitcoin_scamcoin", "LP_Dai_MATIC", "LP_Dai_BNB", "LP_Dai_MintME", "LP_Dai_PolyEUBI", "LP_CLICK_MintME"};
	}
}