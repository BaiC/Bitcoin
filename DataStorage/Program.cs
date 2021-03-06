﻿using System;
using System.Linq;
using System.Threading.Tasks;
using System.Data;
using Dapper;
using MySql.Data.MySqlClient;
using System.Configuration;
using BitfinexAPI;
using System.Threading;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Collections.Generic;

namespace DataStorage
{
    class Program
    {
        static string conStr = ConfigurationManager.ConnectionStrings["mysql"].ConnectionString;
        static DateTime now = DateTime.Now;

        static void Main(string[] args)
        {
            //Run().Wait();
            //RunSocket2();
            Console.WriteLine("请输入获取数据接口类型:(Rest,Socket)");

            while (true)
            {
                var str = Console.ReadLine();
                switch(str)
                {
                    case "Rest":
                        Run().Wait();
                        break;
                    case "Socket":
                        RunSocket2().Wait();
                        break;
                    default:
                        //return;
                        break;
                }
            }

            //Console.ReadKey();
        }

        private static async Task Run()
        {
            var intervals = Enum.GetValues(typeof(KlineInterval));
            var mappingcode = MySql.Data.MySqlClient.MySqlHelper.ExecuteDataRow(conStr, "select * from mappingcode")?.Table;
            var tables = new(string name, KlineInterval[] intervaltype)[]
            {
                ("dailyline",new KlineInterval[]{KlineInterval.OneDay,KlineInterval.OneWeek,KlineInterval.TwoWeeks,KlineInterval.OneMonth }),
                ("hourline",new KlineInterval[]{KlineInterval.OneHour,KlineInterval.ThreeHours,KlineInterval.SixHours,KlineInterval.TwelveHours }),
                ("minuteline",new KlineInterval[]{KlineInterval.OneMinute,KlineInterval.FiveMinutes,KlineInterval.FifteenMinutes,KlineInterval.ThirtyMinutes})
            };

            //foreach (var table in tables)
            //{
            //    using (IDbConnection con = new MySqlConnection(conStr))
            //    {
            //        await con.ExecuteAsync($"delete from {table.name}");
            //        await Console.Out.WriteLineAsync($"delete:{table.name}");
            //    }
            //}

            foreach (var table in tables)
            {
                foreach (DataRow row in mappingcode.Rows)
                {
                    foreach (var interval in table.intervaltype)
                    {
                        await Console.Out.WriteLineAsync($"start:{row["internalcode"]} {interval}");
                        await GetData((string)row["internalcode"], (KlineInterval)interval, table.name);
                        await Console.Out.WriteLineAsync($"end:{row["internalcode"]} {interval}");
                    }
                }
            }
        }

        private static async Task GetData(string code, KlineInterval interval, string table)
        {
            using (IDbConnection con = new MySqlConnection(conStr))
            {
                string symbol = code.Split('.')[0];
                int lastTimeCount = 0;

                bool flag = true;
                var dateTime = new DateTime(2010, 1, 1);
                var intervalint = 0;
                switch (interval)
                {
                    case KlineInterval.OneDay:
                    case KlineInterval.OneHour:
                    case KlineInterval.OneMinute:
                        intervalint = 1;
                        break;
                    case KlineInterval.ThreeHours:
                        intervalint = 3;
                        break;
                    case KlineInterval.SixHours:
                        intervalint = 6;
                        break;
                    case KlineInterval.TwelveHours:
                        intervalint = 12;
                        break;
                    case KlineInterval.FiveMinutes:
                        intervalint = 5;
                        break;
                    case KlineInterval.FifteenMinutes:
                        intervalint = 15;
                        break;
                    case KlineInterval.OneWeek:
                        intervalint = 7;
                        break;
                    case KlineInterval.TwoWeeks:
                        intervalint = 14;
                        break;
                    case KlineInterval.ThirtyMinutes:
                    case KlineInterval.OneMonth:
                        intervalint = 30;
                        break;
                    default:
                        break;
                }

                while (flag)
                {
                    try
                    {
                        Thread.Sleep(5000);

                        BitfinexMethod bitfinexMethod = new BitfinexMethod();

                        await Console.Out.WriteLineAsync($"Request symbol:{symbol} interval:{interval} start:{dateTime} end:{now}");
                        var result = await bitfinexMethod.GetHistoryKlines(symbol, interval, dateTime, now);
                        if (result.Count == 0) return;

                        result.Sort((x, y) => x.timestamp.CompareTo(y.timestamp));
                        await Console.Out.WriteLineAsync($"Result  symbol:{symbol} interval:{interval} start:{result.FirstOrDefault().timestamp} end:{result.LastOrDefault().timestamp} count:{result.Count}");

                        var re = await con.ExecuteAsync($@"insert into {table} (`code`,`interval`,time,open,close,high,low,volume) value
                                                        ('{code}',{intervalint},@timestamp,@open,@close,@high,@low,@volume)
                                                        ON DUPLICATE KEY UPDATE open=@open,close=@close,high=@high,low=@low,volume=@volume", result);

                        if (result.LastOrDefault().timestamp == dateTime)
                        {
                            lastTimeCount++;
                        }
                        dateTime = result.LastOrDefault().timestamp;

                        if (result.Count <= 1) return;
                        if (lastTimeCount > 2) return;
                    }
                    catch (Exception e)
                    {
                        await Console.Out.WriteLineAsync($"Error:{e.Message}");
                        Thread.Sleep(5000);
                    }
                }
            }
        }

        private static async Task RunSocket2()
        {
            using (IDbConnection con = new MySqlConnection(conStr))
            {
                var mappingcode = con.Query<MappingCode>("select * from mappingcode limit 1000");
                var lst = mappingcode.AsEnumerable().Select(_ =>_.InternalCode).ToList();
                
                var dic=lst.ToDictionary(_=> _.Split('.')[0]);
                //var lst = mappingcode.AsEnumerable().ToDictionary(_ => (string)_["internalcode"]);

                BitfinexStream bitfinexStream = new BitfinexStream("");
                Dictionary<int, string> socketPool = new Dictionary<int, string>();

                //string[] lst = new string[] { "BTCUSD", "EOSUSD", "ETHUSD", "BCHUSD", "XRPUSD", "ZECUSD", "LTCUSD", "NEOUSD", "IOTUSD", "TRXUSD", "ETCUSD", "XMRUSD", "OMGUSD", "BTGUSD", "DSHUSD", "EDOUSD", "ETPUSD", "SANUSD", "ELFUSD", "BFTUSD", "QTMUSD", "DATUSD", "ZRXUSD", "XLMUSD" };
                await bitfinexStream.RetrieveKline2(async (klineInfo) =>
                {
                    var j = JsonConvert.DeserializeObject(klineInfo);
                    if (j is JArray)
                    {
                        var chanid = ((JArray)j)[0];
                        var info = ((JArray)j)[1];
                        switch (info.Count())
                        {
                            case 0:
                                break;
                            case 6:
                                Console.WriteLine($"{socketPool[(int)chanid]}:{JsonConvert.SerializeObject(j)}");
                                KlineInfo kline = new KlineInfo()
                                {
                                    timestamp = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(info[0])).DateTime,
                                    open = (decimal)info[1],
                                    close = (decimal)info[2],
                                    high = (decimal)info[3],
                                    low = (decimal)info[4],
                                    volume = (decimal)info[5]
                                };
                                var re = await con.ExecuteAsync($@"insert into minuteline_test (`code`,`interval`,time,open,close,high,low,volume) value
                                                        ('{socketPool[(int)chanid]}',1,@timestamp,@open,@close,@high,@low,@volume)
                                                        ON DUPLICATE KEY UPDATE open=@open,close=@close,high=@high,low=@low,volume=@volume", kline);
                                break;
                            default:
                                break;
                        }
                    }
                    else if (j is JObject)
                    {
                        JObject jObject = (JObject)j;
                        if (jObject["event"].ToString() == "subscribed")
                        {
                            var key = jObject["key"].ToString();
                            socketPool.Add((int)jObject["chanId"], dic[key.Substring(key.Length - 6)]);
                            await Console.Out.WriteLineAsync($"event:{jObject["event"]} channel:{jObject["channel"]} chanId:{jObject["chanId"]} key:{jObject["key"]}");
                        }
                    }
                }, lst.ToList());
            }
        }
    }

    public class MappingCode
    {
        public string InternalCode { get; set; }
        public string ExternalCode { get; set; }
        public string StartDate { get; set; }
        public string Flag { get; set; }
    }
}
