using System;
using System.Collections.Concurrent;
using System.Threading;

namespace jessielesbian.OpenCEX
{
	/// <summary>
	/// Pooled Manual Reset Events pools unmanaged resources behind manual reset events
	/// </summary>
	public sealed class PooledManualResetEvent : IDisposable
	{
		private static readonly ConcurrentBag<ManualResetEventSlim> manualResetEventSlims = new ConcurrentBag<ManualResetEventSlim>();
		private readonly ManualResetEventSlim manualResetEventSlim;
		private volatile int notDisposed = 0;
		private static readonly object sync2 = new object();

		public static PooledManualResetEvent GetInstance(bool signaled){
			if(manualResetEventSlims.TryTake(out ManualResetEventSlim result)){
				return new PooledManualResetEvent(result, !signaled);
			} else{
				ManualResetEventSlim temp = new ManualResetEventSlim(signaled);
				GC.SuppressFinalize(temp);
				return new PooledManualResetEvent(temp, false);
			}
		}
		private PooledManualResetEvent(ManualResetEventSlim manualResetEventSlim, bool reset)
		{
			this.manualResetEventSlim = manualResetEventSlim ?? throw new ArgumentNullException(nameof(manualResetEventSlim));
			
			if(reset){
				Reset();
			}
		}

		public void Set()
		{
			lock(manualResetEventSlim){
				if(!manualResetEventSlim.IsSet){
					manualResetEventSlim.Set();
				}
			}
		}

		public void Reset()
		{
			lock (manualResetEventSlim)
			{
				if (manualResetEventSlim.IsSet)
				{
					manualResetEventSlim.Reset();
				}
			}
		}

		public void Cycle()
		{
			lock (manualResetEventSlim)
			{
				if (!manualResetEventSlim.IsSet)
				{
					manualResetEventSlim.Set();
					manualResetEventSlim.Reset();
				}
			}
		}

		public void Wait()
		{
			manualResetEventSlim.Wait();
		}

		public void Wait(int timeout){
			manualResetEventSlim.Wait(timeout);
		}

		private void Dispose(bool disposing)
		{
			if (Interlocked.Exchange(ref notDisposed, 1) == 0)
			{
				if(disposing){
					Set();
				} else if(!manualResetEventSlim.IsSet){
					//since we only have 1 finalizer thread, we skip locking.
					manualResetEventSlim.Set();
				}


				bool dispose;
				lock(sync2){
					if(manualResetEventSlims.Count < 10000){
						manualResetEventSlims.Add(manualResetEventSlim);
						dispose = false;
					} else{
						dispose = true;
					}
				}
				if(dispose){
					manualResetEventSlim.Dispose();
				}
			}
		}

		public bool IsSet => manualResetEventSlim.IsSet;

		// TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
		~PooledManualResetEvent()
		{
			// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
			Dispose(disposing: false);
		}

		public void Dispose()
		{
			// Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
			Dispose(disposing: true);
			GC.SuppressFinalize(this);
		}
	}
}