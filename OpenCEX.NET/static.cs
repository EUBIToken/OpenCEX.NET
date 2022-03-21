using System;
using System.Collections.Generic;
using System.Net;
using Newtonsoft.Json;
using System.Runtime.Serialization;
using System.Threading;
using jessielesbian.OpenCEX.RequestManager;
using MySql.Data.MySqlClient;
using System.IO;

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

		

		public static MySqlTransaction GetSQL(){
			MySqlConnection mySqlConnection = null;
			try
			{
				mySqlConnection = new MySqlConnection(GetEnv("SQLConnectionString"));
				MySqlTransaction tx = mySqlConnection.BeginTransaction();

				
				CheckSafety(tx, "MySQL connection establishment failed: invalid MySQL transaction object!");
				
				return tx;
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
		private static readonly HttpListener httpListener;
		private static readonly ManualResetEventSlim terminateMainThread = new ManualResetEventSlim();
		private static bool dispose = true;
		static StaticUtils(){
			httpListener = new HttpListener();
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
		private sealed class FailedRequest{
			private readonly string status = "error";
			private readonly string reason;

			public FailedRequest(string reason)
			{
				this.reason = reason ?? throw new ArgumentNullException(nameof(reason));
			}
		}
		public static readonly bool debug = Convert.ToBoolean(GetEnv("Debug"));
		public static void HandleHTTPRequest(HttpListenerContext httpListenerContext){
			
			try{
				HttpListenerRequest httpListenerRequest = httpListenerContext.Request;
				HttpListenerResponse httpListenerResponse = httpListenerContext.Response;
				StreamWriter streamWriter = new StreamWriter(new BufferedStream(httpListenerResponse.OutputStream, 65536));
				try
				{

					//POST requests only
					CheckSafety(httpListenerRequest.HttpMethod == "POST", "Illegal request method!");

					//CSRF protection
					CheckSafety(httpListenerRequest.Headers.Get("Origin") == GetEnv("Origin"), "Illegal origin!");
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