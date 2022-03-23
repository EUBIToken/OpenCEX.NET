using jessielesbian.OpenCEX.SafeMath;
using System.Numerics;
using System.Collections;
using System.Linq;
using System;
using MySql.Data.MySqlClient;
using System.Security.Cryptography;
using System.Net;
using jessielesbian.OpenCEX.RequestManager;

namespace jessielesbian.OpenCEX
{
	public static class SessionManager
	{
		public static ulong GetUserID(this Request request, bool thr = true){
			Cookie cookie = request.httpListenerContext.Request.Cookies["OpenCEX_session"];
			if(cookie == null){
				StaticUtils.CheckSafety2(thr, "Missing session token!");
				return 0;
			}

			byte[] bytes;
			try{
				bytes = Convert.FromBase64String(cookie.Value);
			} catch{
				StaticUtils.CheckSafety2(thr, "Invalid session token!");
				return 0;
			}

			StaticUtils.CheckSafety(bytes.Length == 64, "Invalid session token!");
			SHA256 hash = SHA256.Create();
			string result = BitConverter.ToString(hash.ComputeHash(bytes)).Replace("-", string.Empty);
			hash.Dispose();

			MySqlDataReader reader = request.sqlCommandFactory.GetCommand("SELECT UserID, Expiry FROM Sessions WHERE SessionTokenHash = " + result + ";").ExecuteReader();

			int len = reader.FieldCount;
			if(len == 0){
				StaticUtils.CheckSafety2(thr, "Invalid session token!");
				return 0;
			} else{
				StaticUtils.CheckSafety(len == 1, "Corrupted sessions table!");
				if(reader.GetInt64("Expiry") > DateTimeOffset.UtcNow.ToUnixTimeSeconds()){
					StaticUtils.CheckSafety2(thr, "Session token expired!");
				}
				ulong ret = reader.GetUInt64("UserID");
				reader.Close();
				return ret;
			}


		}
	}
}
