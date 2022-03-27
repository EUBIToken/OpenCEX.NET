using MySql.Data.MySqlClient;
using Nethereum.RPC.Eth.DTOs;
using System;
using System.Collections.Generic;

using System.Threading;
namespace jessielesbian.OpenCEX{
	public static partial class StaticUtils{
		private static void DepositManager(){
			while(!abort){
				if(watchdogSoftReboot){
					Thread.Sleep(1);
					continue;
				} else{
					MySqlConnection mySqlConnection = new MySqlConnection(SQLConnectionString);
					try
					{
						Exception delayed_throw = null;
						mySqlConnection.Open();
						MySqlTransaction mySqlTransaction = mySqlConnection.BeginTransaction(); //Read-only transaction
						try
						{
							delayed_throw = HandleDepositsIMPL(new MySqlCommand("SELECT LastTouched, URL, URL2, Id FROM WorkerTasks;", mySqlConnection, mySqlTransaction).ExecuteReader());
						}
						catch (Exception e)
						{
							delayed_throw = new SafetyException("Unable to get pending deposits handle!", e);
						}
						mySqlTransaction.Rollback();
						if (delayed_throw != null)
						{
							throw delayed_throw;
						}
					}
					catch (Exception e)
					{
						Console.Error.WriteLine("Exception in deposit manager thread: " + e.ToString());
					}
					finally
					{
						mySqlConnection.Close();
					}
				}
			}
		}
		private static Exception HandleDepositsIMPL(MySqlDataReader mySqlDataReader){
			Exception deferredThrow = null;
			try{
				Queue<ConcurrentJob> queue = new Queue<ConcurrentJob>();
				while(mySqlDataReader.Read()){
					queue.Enqueue(new TryProcessDeposit(mySqlDataReader.GetUInt64("LastTouched"), mySqlDataReader.GetString("URL"), mySqlDataReader.GetString("URL2"), mySqlDataReader.GetUInt64("Id")));
				}
				ConcurrentJob[] arr = queue.ToArray();
				Append(arr);
				foreach(ConcurrentJob concurrentJob in arr){
					concurrentJob.Wait();
				}

			} catch (Exception e){
				deferredThrow = new SafetyException("Exception in deposit manager core!", e);
			}
			mySqlDataReader.Dispose();
			return deferredThrow;
		}
		private sealed class TryProcessDeposit : ConcurrentJob
		{
			private readonly ulong userid;
			private readonly string url1;
			private readonly string url2;
			private readonly ulong id;

			public TryProcessDeposit(ulong userid, string url1, string url2, ulong id)
			{
				this.userid = userid;
				this.url1 = url1 ?? throw new ArgumentNullException(nameof(url1));
				this.url2 = url2 ?? throw new ArgumentNullException(nameof(url2));
				this.id = id;
			}

			protected override object ExecuteIMPL()
			{
				if ((url1 is null) || (url2 is null) || userid == 0){
					return null;
				}
				//[txid, amount]
				string[] misc = url2.Split('_');
				WalletManager walletManager;
				switch(url1){
					case "MintME":
						walletManager = BlockchainManager.MintME.GetWalletManager();
						break;
					case "BNB":
						walletManager = BlockchainManager.BinanceSmartChain.GetWalletManager();
						break;
					case "MATIC":
						walletManager = BlockchainManager.BinanceSmartChain.GetWalletManager();
						break;
					default:
						throw new Exception("Unknown token!");
				}

				Transaction transaction = walletManager.GetTransactionReceipt(misc[0]);
				if(transaction != null){
					Console.WriteLine(transaction.BlockNumber.ToString());
				}
				return null;
			}
		}
	}
}
