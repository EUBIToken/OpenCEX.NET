using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;

namespace jessielesbian.OpenCEX{
	public sealed class ConcurrentDualGenerationCache<K, T>{
		private Dictionary<K, T> gen1;
		private Dictionary<K, T> gen2;
		private ConcurrentDictionary<K, ReaderWriterLockSlim> locks;
		private readonly int maxsize;
		private readonly object lok = new object();

		public ConcurrentDualGenerationCache(int maxsize){
			StaticUtils.CheckSafety2(maxsize < 1, "Invalid cache size (should not reach here)!", true);
			this.maxsize = maxsize;
			locks = new ConcurrentDictionary<K, ReaderWriterLockSlim>(StaticUtils.thrlimit, maxsize * 2);
			gen1 = new Dictionary<K, T>(maxsize);
			gen2 = new Dictionary<K, T>(maxsize);
		}

		private static ReaderWriterLockSlim GetLock(K _){
			return new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);
		}
		public T GetOrAdd(K key, Func<K, T> generate, out bool called){
			StaticUtils.CheckSafety(locks.TryGetValue(key, out ReaderWriterLockSlim rwlock), "Cache lock not acquired (should not reach here)!", true);
			StaticUtils.CheckSafety(rwlock.IsReadLockHeld || rwlock.IsWriteLockHeld, "Cache lock not acquired (should not reach here)!", true);

			lock (lok)
			{
				if (gen1.TryGetValue(key, out T ret))
				{
					called = false;
					return ret;
				}
				else if (gen2.TryGetValue(key, out ret))
				{
					called = false;
					if (maxsize > gen1.Count)
					{
						//Move from generation 2 to generation 1, since entry became live again.
						StaticUtils.CheckSafety(gen1.TryAdd(key, ret), "Unable to promote cache entry (should not reach here)!", true);
						StaticUtils.CheckSafety(gen2.Remove(key), "Unable to move promoted cache entry (should not reach here)!", true);
					}
					return ret;
				}
				else
				{
					//First, we try to add to generation 1
					//If generation 1 is full, we try generation 2
					called = true;
					ret = generate.Invoke(key);
					if (gen1.Count == maxsize)
					{
						Dictionary<K, T> swap = gen2;
						//If generation 2 is full, perform cache swap
						//and add to generation 1
						if(swap.Count == maxsize){
							swap.Clear();
							gen2 = gen1;
							gen1 = swap;
						} else{
							StaticUtils.CheckSafety(gen2.TryAdd(key, ret), "Unable to add to second-generation cache (should not reach here)!", true);
						}
					} else{
						StaticUtils.CheckSafety(gen1.TryAdd(key, ret), "Unable to add to first-generation cache (should not reach here)!", true);
					}
					return ret;
				}
			}
			
		}

		public void UpdateOrAdd(K key, T val){
			StaticUtils.CheckSafety(locks.TryGetValue(key, out ReaderWriterLockSlim rwlock), "Cache write-lock not acquired (should not reach here)!", true);
			StaticUtils.CheckSafety(rwlock.IsWriteLockHeld, "Cache write-lock not acquired (should not reach here)!", true);
			lock(lok){
				if (gen1.Count == maxsize)
				{
					if (gen1.ContainsKey(key))
					{
						gen1[key] = val;
					}
					else
					{
						Dictionary<K, T> swap = gen2;

						//If generation 2 is full
						//And we are not already cached
						//Perform cache swap
						if (swap.Count == maxsize)
						{
							if (swap.ContainsKey(key))
							{
								swap[key] = val;
							} else{
								swap.Clear();
								gen2 = gen1;
								gen1 = swap;
							}	
						}
						else if(!swap.TryAdd(key, val))
						{
							swap[key] = val;
						}
					}
					
				}
				else if (!gen1.TryAdd(key, val))
				{
					gen1[key] = val;
				}
			}
		}

		public void LockWrite(K key){
			ReaderWriterLockSlim rwlock = locks.GetOrAdd(key, GetLock);
			StaticUtils.CheckSafety2(rwlock.IsReadLockHeld || rwlock.IsWriteLockHeld, "Cache lock already acquired (should not reach here)!", true);
			rwlock.EnterWriteLock();
		}

		public void LockRead(K key)
		{
			ReaderWriterLockSlim rwlock = locks.GetOrAdd(key, GetLock);
			StaticUtils.CheckSafety2(rwlock.IsReadLockHeld || rwlock.IsWriteLockHeld, "Cache lock already acquired (should not reach here)!", true);
			rwlock.EnterReadLock();
		}

		public void Unlock(K key){
			StaticUtils.CheckSafety(locks.TryGetValue(key, out ReaderWriterLockSlim rwlock), "Cache lock not acquired (should not reach here)!", true);
			if(rwlock.IsReadLockHeld){
				rwlock.ExitReadLock();
			} else if(rwlock.IsWriteLockHeld){
				rwlock.ExitWriteLock();
			} else{
				throw new SafetyException("Cache lock not acquired (should not reach here)!", new Exception("Cache lock not acquired (should not reach here)!"));
			}
		}

		public void Clear(K key){
			lock(lok){
				if(!gen1.Remove(key)){
					if(!gen2.Remove(key)){
						gen1.Clear();
						gen2.Clear();
					}
				}
			}
		}
		public void Clear()
		{
			lock (lok)
			{
				gen1.Clear();
				gen2.Clear();
			}
		}
	}
}
