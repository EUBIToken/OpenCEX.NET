using System;
using jessielesbian.OpenCEX.SafeMath;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using jessielesbian.OpenCEX.oracle;
using System.Threading;
using System.Collections.Concurrent;

namespace jessielesbian.OpenCEX
{
	namespace oracle{
		public interface IOracle{
			public SafeUint GetPriceAt(ulong time, ulong tolerance);
			public void Flush();
		}
		public sealed class OracleFlushingJob : ConcurrentJob{
			private readonly IOracle oracle;

			public OracleFlushingJob(IOracle oracle)
			{
				this.oracle = oracle ?? throw new ArgumentNullException(nameof(oracle));
			}

			protected override object ExecuteIMPL()
			{
				oracle.Flush();
				return null;
			}
		}

		/// <summary>
		/// CoinMarketCrack oracle bypasses CoinMarketCap API limits with intelligent schemanigans
		/// </summary>
		public sealed class CoinMarketCrackOracle : IOracle{

			[JsonObject(MemberSerialization.Fields)]
			private sealed class CMCOracleModel2
			{
#pragma warning disable CS0649
				public double[] v;
#pragma warning restore CS0649
			}
			[JsonObject(MemberSerialization.Fields)]
			private sealed class CMCOracleModel{
#pragma warning disable CS0649
				public Dictionary<string, Dictionary<string, CMCOracleModel2>> data;
				public Dictionary<string, object> status;
#pragma warning restore CS0649
			}
			private readonly Uri apicall;
			private readonly IDictionary<ulong, SafeUint> CachedPrices = new Dictionary<ulong, SafeUint>();
			private readonly ConcurrentDictionary<ulong, SafeUint> CachedPrices2 = new ConcurrentDictionary<ulong, SafeUint>();
			private readonly object locker = new object();
			private readonly HttpClient httpClient = new HttpClient();
			private readonly int length;
			private readonly string filler;
			public CoinMarketCrackOracle(ulong coinid, uint length){
				apicall = new Uri("https://api.coinmarketcap.com/data-api/v3/cryptocurrency/detail/chart?id=" + coinid.ToString() + "&range=1M");
				this.length = (int)length;
				filler = string.Intern(new string('0', this.length));
				httpClient.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36");
				httpClient.DefaultRequestHeaders.Add("Accept", "*/*");
				httpClient.DefaultRequestHeaders.Add("Host", "api.coinmarketcap.com");
			}

			public void Flush(){
				StaticUtils.CheckSafety2(StaticUtils.Multiserver, "This oracle is not multiserver-safe!");
				
				//Get data
				Task<string> tsk;
				lock(httpClient)
				{
					tsk = httpClient.GetStringAsync(apicall);
				}

				//Deserialize
				CMCOracleModel output = JsonConvert.DeserializeObject<CMCOracleModel>(StaticUtils.Await2(tsk), StaticUtils.oracleJsonSerializerSettings);

				//Check safety
				StaticUtils.CheckSafety(output.status.TryGetValue("error_message", out object tmp), "Missing CoinMarketCap error message!");
				if(tmp is string msg){
					StaticUtils.CheckSafety(msg == "SUCCESS", "CoinMarketCap oracle returned error: " + msg);
				} else{
					throw new SafetyException("CoinMarketCap error message is not string!");
				}

				//More safety checks
				StaticUtils.CheckSafety(output.data.TryGetValue("points", out Dictionary<string, CMCOracleModel2> l2), "Missing CoinMarketCap price chart!");
				StaticUtils.CheckSafety(l2, "CoinMarketCap oracle returned null price chart!");

				CachedPrices2.Clear();
				foreach (KeyValuePair<string, CMCOracleModel2> keyValuePair in l2)
				{
					ulong time = Convert.ToUInt64(keyValuePair.Key);
					if(!CachedPrices.ContainsKey(time)){
						string[] parts = keyValuePair.Value.v[0].ToString("r").Split('.');
						string work = parts[0];
						if (parts.Length == 2)
						{
							int l3 = parts[1].Length;
							if (l3 > length)
							{
								work += parts[1].Substring(0, length);
							}
							else
							{
								work += parts[1] + new string('0', length - l3);
							}
						}
						else
						{
							work += filler;
						}

						StaticUtils.CheckSafety(CachedPrices.TryAdd(time, StaticUtils.GetSafeUint(work)), "Unable to update CoinMarketCap price cache!");
					}
				}
			}

			public SafeUint GetPriceAt(ulong time, ulong tolerance)
			{
				StaticUtils.CheckSafety2(StaticUtils.Multiserver, "This oracle is not multiserver-safe!");
				
				return CachedPrices2.GetOrAdd(time, (ulong _) => {
					ulong lastDist = ulong.MaxValue;
					SafeUint ret = null;
					lock (locker)
					{	
						foreach (KeyValuePair<ulong, SafeUint> keyValuePair in CachedPrices)
						{
							if (time >= keyValuePair.Key)
							{
								ulong dist = time - keyValuePair.Key;
								if (dist < lastDist && dist < tolerance)
								{
									ret = keyValuePair.Value;
									lastDist = dist;
								}
							}
							
						}
						if (lastDist < ulong.MaxValue)
						{
							StaticUtils.CheckSafety(CachedPrices2.TryAdd(time, ret), "Unable to append CoinMarketCap price to L2 cache!");
						}
					}
					return ret;
				});
			}
		}
	}
	public static partial class StaticUtils
	{
		private static readonly Dictionary<string, IOracle> oracles = new Dictionary<string, IOracle>();
		
		public static void OracleThread(){
			while(!abort){
				try{
					foreach(KeyValuePair<string, IOracle> keyValuePair in oracles){
						try{
							keyValuePair.Value.Flush();
						}
						catch (Exception e)
						{
							Console.WriteLine("Exception in oracle thread: " + e);
						}
					}
				} catch (Exception e){
					Console.WriteLine("Exception in oracle thread: " + e);
				}
				Thread.Sleep(10000);
			}
			Console.WriteLine(Thread.CurrentThread.Name + " stopped!");
		}
	}
}
