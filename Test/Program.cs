using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net.Http;

using BitfinexAPI;
using BinanceAPI;

using WebSocketSharp;
using Newtonsoft.Json;

namespace Test
{
    class Program
    {
        static void Main(string[] args)
        {
            string apiKey = ConfigurationManager.AppSettings["ApiKey"];
            string secretKey = ConfigurationManager.AppSettings["SecretKey"];

            DateTime start = DateTime.Now.AddDays(-150);
            DateTime end = start.AddMinutes(10);

            BitfinexMethod bm = new BitfinexMethod(null, null);
            var r = bm.GetSymbols().Result;

            Console.ReadKey();
        }
    }
}
