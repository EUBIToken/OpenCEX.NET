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
				bytes = Convert.FromBase64String(WebUtility.UrlDecode(cookie.Value));
			} catch{
				StaticUtils.CheckSafety2(thr, "Invalid session token!");
				return 0;
			}

			StaticUtils.CheckSafety(bytes.Length == 64, "Invalid session token!");
			SHA256 hash = SHA256.Create();
			string result = BitConverter.ToString(hash.ComputeHash(bytes)).Replace("-", string.Empty);
			hash.Dispose();

			MySqlDataReader reader = request.sqlCommandFactory.SafeExecuteReader(request.sqlCommandFactory.GetCommand("SELECT UserID, Expiry FROM Sessions WHERE SessionTokenHash = \"" + result + "\";"));

			ulong ret = 0;
			if (reader.HasRows)
			{
				if (reader.GetInt64("Expiry") > DateTimeOffset.UtcNow.ToUnixTimeSeconds())
				{
					ret = reader.GetUInt64("UserID");
					StaticUtils.CheckSafety2(ret == 0, "Illegal User ID!");
					reader.CheckSingletonResult();
				} else{
					StaticUtils.CheckSafety2(thr, "Session token expired!");
				}
				
			}
			else
			{
				StaticUtils.CheckSafety2(thr, "Invalid session token!");
			}
			request.sqlCommandFactory.SafeDestroyReader();
			return ret;
		}
	}
}
