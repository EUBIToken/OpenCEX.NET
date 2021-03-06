using MySql.Data.MySqlClient;
using Nethereum.Hex.HexTypes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
namespace jessielesbian.OpenCEX{
	public static partial class StaticUtils{
		public static readonly WalletManager defaultMintMEWallet = BlockchainManager.MintME.ExchangeWalletManager;
		public static readonly WalletManager defaultBSCWallet = BlockchainManager.BinanceSmartChain.ExchangeWalletManager;
		public static readonly WalletManager defaultPolyWallet = BlockchainManager.Polygon.ExchangeWalletManager;
		private static void DepositManager(){
			while (!abort)
			{
				if (watchdogCounter > MaximumWatchdogLag)
				{
					Thread.Sleep(3);
					continue;
				}
				else
				{
					ConcurrentJob[] updates = new ConcurrentJob[] { defaultMintMEWallet.GetUpdate(), defaultBSCWallet.GetUpdate(), defaultPolyWallet.GetUpdate() };
					Append(updates);
					try
					{
						SQLCommandFactory sqlCommandFactory = GetSQL(IsolationLevel.RepeatableRead);
						try
						{
							HandleDepositsIMPL(sqlCommandFactory.GetCommand("SELECT LastTouched, URL, URL2, Id FROM WorkerTasks;").ExecuteReader(), updates);
							sqlCommandFactory.Dispose();
						}
						catch (Exception e)
						{
							sqlCommandFactory.Dispose();
							throw new SafetyException("Unable to get pending deposits handle!", e);
						}
					}
					catch (Exception e)
					{
						Console.Error.WriteLine("Exception in deposit manager thread: " + e.ToString());
					}
				}
				Thread.Sleep(10000);
				if (!Multiserver)
				{
					depositBlocker.Wait();
				}
			}
			Console.WriteLine(Thread.CurrentThread.Name + " stopped!");
		}

		private static readonly ConcurrentJob[] empty = new ConcurrentJob[0];
		private static void HandleDepositsIMPL(MySqlDataReader mySqlDataReader, ConcurrentJob[] updates)
		{
			ConcurrentJob[] arr = empty;
			Exception deferredThrow = null;
			try{
				Queue<ConcurrentJob> queue = new Queue<ConcurrentJob>();
				while(mySqlDataReader.Read()){
					queue.Enqueue(new TryProcessDeposit(mySqlDataReader.GetUInt64("LastTouched"), mySqlDataReader.GetString("URL"), mySqlDataReader.GetString("URL2"), mySqlDataReader.GetUInt64("Id")));
				}
				if(queue.Count == 0){
					if(!Multiserver){
						depositBlocker.Reset();
					}

					//Fast reset
					mySqlDataReader.Close();
					return;
				} else{
					arr = queue.ToArray();
					if(Multiserver){
						Append(arr);
					}
					
					try
					{
						mySqlDataReader.Close();
					}
					finally
					{
						mySqlDataReader = null;
					}
				}
				
			} catch (Exception e){
				deferredThrow = new SafetyException("Exception in deposit manager core!", e);
			}
			if(!(mySqlDataReader is null)){
				mySqlDataReader.Close();
			}
			if(deferredThrow == null){
				try{
					foreach (ConcurrentJob concurrentJob3 in updates)
					{
						concurrentJob3.Wait();
					}
					if (!Multiserver)
					{
						Append(arr);
					}
					foreach (ConcurrentJob concurrentJob4 in arr)
					{
						concurrentJob4.Wait();
					}
				} catch (Exception e){
					throw new SafetyException("Exception while finalizing deposits", e);
				}
			} else{
				throw deferredThrow;
			}
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
				bool backed;
				string url3 = url1;
				switch(url3)
				{
					case "MintME":
					case "EUBI":
					case "1000x":
					case "CLICK":
					case "Haoma":
					case "MS-Coin":
						walletManager = BlockchainManager.MintME.ExchangeWalletManager;
						backed = false;
						break;
					case "BNB":
						walletManager = BlockchainManager.BinanceSmartChain.ExchangeWalletManager;
						backed = false;
						break;
					case "MATIC":
					case "PolyEUBI":
					case "Dai":
						walletManager = BlockchainManager.Polygon.ExchangeWalletManager;
						backed = false;
						break;
					case "WMintME":
						walletManager = BlockchainManager.Polygon.ExchangeWalletManager;
						backed = true;
						url3 = "MintME";
						break;
					default:
						ThrowInternal2("Unknown token in Deposit Manager (should not reach here)!");
						return null;
				}

				TransactionReceipt transaction = walletManager.GetTransactionReceipt("0x" + misc[0]);
				ulong safeheight = walletManager.SafeBlockheight;
				if (safeheight == 0){
					if(!Multiserver){
						depositBlocker.Set();
					}
					return null; //Later
				}
				if(!(transaction is null)){
					if(!(transaction.blockNumber is null)){
						if(safeheight > Convert.ToUInt64(GetSafeUint(Convert.ToString(transaction.blockNumber)).ToString()))
						{
							SQLCommandFactory sqlCommandFactory = GetSQL(IsolationLevel.RepeatableRead);
							Exception deferred = null;
							try
							{
								try{
									sqlCommandFactory.GetCommand("SELECT NULL FROM WorkerTasks WHERE Id = " + id + " FOR UPDATE NOWAIT;").ExecuteNonQuery();
								} catch{
									return null;
								}
								
								sqlCommandFactory.SafeExecuteNonQuery("DELETE FROM WorkerTasks WHERE Id = " + id + ";");
								if (GetSafeUint(Convert.ToString(transaction.status)) == one)
								{
									//UNSAFE credit, since we are adding newly-deposited funds
									sqlCommandFactory.Credit(url3, userid, GetSafeUint(misc[1]), backed);
								}
								sqlCommandFactory.DestroyTransaction(true, true);
							}
							catch (Exception e)
							{
								deferred = new SafetyException("Exception in deposit crediting function!", e);
							}
							sqlCommandFactory.Dispose();
							if(!(deferred is null)){
								throw deferred;
							}
						}
					}
				}
				return null;
			}
		}
	}
}
