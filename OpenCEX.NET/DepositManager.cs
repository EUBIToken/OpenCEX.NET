using MySql.Data.MySqlClient;
using Nethereum.Hex.HexTypes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;

using System.Threading;
namespace jessielesbian.OpenCEX{
	public static partial class StaticUtils{
		public static readonly WalletManager defaultMintMEWallet = BlockchainManager.MintME.GetWalletManager();
		public static readonly WalletManager defaultBSCWallet = BlockchainManager.BinanceSmartChain.GetWalletManager();
		public static readonly WalletManager defaultPolyWallet = BlockchainManager.Polygon.GetWalletManager();
		private static void DepositManager(){
			while(!abort){
				if(watchdogSoftReboot){
					Thread.Sleep(1001);
					continue;
				} else{
					ConcurrentJob[] updates = new ConcurrentJob[] { defaultMintMEWallet.update, defaultBSCWallet.update, defaultPolyWallet.update };
					Append(updates);
					MySqlConnection mySqlConnection = new MySqlConnection(SQLConnectionString);
					try
					{
						Exception delayed_throw = null;
						mySqlConnection.Open();
						MySqlTransaction mySqlTransaction = mySqlConnection.BeginTransaction(); //Read-only transaction
						try
						{
							delayed_throw = HandleDepositsIMPL(new MySqlCommand("SELECT LastTouched, URL, URL2, Id FROM WorkerTasks;", mySqlConnection, mySqlTransaction).ExecuteReader(), updates);
						}
						catch (Exception e)
						{
							delayed_throw = new SafetyException("Unable to get pending deposits handle!", e);
						}
						mySqlTransaction.Rollback();
						mySqlTransaction.Dispose();
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
				Thread.Sleep(10000);
			}
		}

		private static ConcurrentJob[] empty = new ConcurrentJob[0];
		private static Exception HandleDepositsIMPL(MySqlDataReader mySqlDataReader, ConcurrentJob[] updates)
		{
			ConcurrentJob[] arr = empty;
			Exception deferredThrow = null;
			try{
				Queue<ConcurrentJob> queue = new Queue<ConcurrentJob>();
				while(mySqlDataReader.Read()){
					queue.Enqueue(new TryProcessDeposit(mySqlDataReader.GetUInt64("LastTouched"), mySqlDataReader.GetString("URL"), mySqlDataReader.GetString("URL2"), mySqlDataReader.GetUInt64("Id")));
				}
				arr = queue.ToArray();
				Append(arr);
				try {
					mySqlDataReader.Close();
				} finally{
					mySqlDataReader = null;
				}
			} catch (Exception e){
				deferredThrow = new SafetyException("Exception in deposit manager core!", e);
			}
			if(!(mySqlDataReader is null)){
				mySqlDataReader.Close();
			}
			if(deferredThrow == null){
				try{
					foreach (ConcurrentJob concurrentJob in updates)
					{
						concurrentJob.Wait();
					}
					foreach (ConcurrentJob concurrentJob in arr)
					{
						concurrentJob.Wait();
					}
				} catch (Exception e){
					return e;
				}
			}
			
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
				CheckSafety2(userid == 0, "Deposit to illegal UserID!");
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

				TransactionReceipt transaction = walletManager.GetTransactionReceipt(misc[0]);
				if(!(transaction is null)){
					if(!(transaction.blockNumber is null)){
						Console.WriteLine(Convert.ToString(transaction.blockNumber));
						if(Convert.ToUInt64(GetSafeUint(Convert.ToString(transaction.blockNumber)).ToString()) > walletManager.SafeBlockheight)
						{
							SQLCommandFactory sqlCommandFactory = GetSQL();
							try{
								sqlCommandFactory.SafeExecuteNonQuery("DELETE FROM WorkerTasks WHERE Id = " + id + ";");
								//UNSAFE credit, since we are adding newly-deposited funds
								sqlCommandFactory.Credit(url1, userid, GetSafeUint(misc[1]), false);
								sqlCommandFactory.DestroyTransaction(true, true);
							} finally{
								sqlCommandFactory.Dispose();
							}
						}
					}
				}
				return null;
			}
		}
	}
}
