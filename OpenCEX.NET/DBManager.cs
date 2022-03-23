using MySql.Data.MySqlClient;
using System;

namespace jessielesbian.OpenCEX{
	public sealed class SQLCommandFactory : IDisposable
	{
		private readonly MySqlConnection mySqlConnection;
		private MySqlTransaction mySqlTransaction;
		private bool disposedValue;
		private MySqlDataReader dataReader = null;

		public SQLCommandFactory(MySqlConnection mySqlConnection, MySqlTransaction mySqlTransaction)
		{
			this.mySqlConnection = mySqlConnection ?? throw new ArgumentNullException(nameof(mySqlConnection));
			this.mySqlTransaction = mySqlTransaction ?? throw new ArgumentNullException(nameof(mySqlTransaction));
		}

		public MySqlDataReader SafeExecuteReader(MySqlCommand mySqlCommand){
			StaticUtils.CheckSafety2(dataReader, "Data reader already created!");
			MySqlDataReader temp = mySqlCommand.ExecuteReader();
			temp.Read();
			dataReader = temp;
			return temp;
		}

		public void SafeDestroyReader(){
			StaticUtils.CheckSafety(dataReader, "Data reader already destroyed!");
			dataReader.Close();
			dataReader = null;
		}

		private void RequireTransaction()
		{
			StaticUtils.CheckSafety2(disposedValue, "MySQL connection already disposed!");
			StaticUtils.CheckSafety(mySqlTransaction, "MySQL transaction not open!");
		}

		public MySqlCommand GetCommand(string cmd)
		{
			RequireTransaction();
			return new MySqlCommand(cmd, mySqlConnection, mySqlTransaction);
		}

		public void DestroyTransaction(bool commit, bool destroy)
		{
			RequireTransaction();
			try
			{
				if (commit)
				{
					StaticUtils.CheckSafety2(dataReader, "Data reader still open!");
					mySqlTransaction.Commit();
					mySqlTransaction = null;
				}
				else
				{
					try{
						if (dataReader != null)
						{
							SafeDestroyReader();
						}
						mySqlTransaction.Rollback();
						mySqlTransaction.Dispose();
					} finally{
						mySqlTransaction = null;
					}
					
				}

				if (destroy)
				{
					Dispose();
				}

			}
			catch (Exception e)
			{
				if (StaticUtils.debug)
				{
					throw e;
				}
				else
				{
					throw new SafetyException("Unable to destroy MySQL transaction!");
				}

			}
		}

		public void BeginTransaction()
		{
			StaticUtils.CheckSafety2(disposedValue, "MySQL connection already disposed!");
			StaticUtils.CheckSafety2(mySqlTransaction, "MySQL transaction already exist!");
			mySqlTransaction = mySqlConnection.BeginTransaction();
		}

		public void Dispose()
		{
			GC.SuppressFinalize(this);
			if (!disposedValue)
			{
				if (mySqlTransaction != null)
				{
					DestroyTransaction(false, true);
				}
				else
				{
					mySqlConnection.Close();

					// TODO: free unmanaged resources (unmanaged objects) and override finalizer
					// TODO: set large fields to null
					disposedValue = true;
				}
			}
		}

		~SQLCommandFactory()
		{
			Dispose();
		}
	}
}
