using System;
using System.Linq;
using System.Threading.Tasks;
using System.Data;
using Dapper;
using MySql.Data.MySqlClient;
using System.Configuration;
using BitfinexAPI;
using System.Threading;
using Newtonsoft.Json.Linq;

namespace DataStorage
{
    class Program
    {
        static string conStr = ConfigurationManager.ConnectionStrings["mysql"].ConnectionString;
        static DateTime now = DateTime.Now;

        static void Main(string[] args)
        {
            Run().Wait();

            Console.WriteLine("end...");
            Console.ReadKey();
        }

        private static async Task Run()
        {           
            var intervals = Enum.GetValues(typeof(KlineInterval));
            var symbols = new string[] { "btcusd", "ltcusd", "ltcbtc", "ethusd", "ethbtc", "etcbtc", "etcusd", "rrtusd", "rrtbtc", "zecusd", "zecbtc", "xmrusd", "xmrbtc", "dshusd", "dshbtc", "btceur", "btcjpy", "xrpusd", "xrpbtc", "iotusd", "iotbtc", "ioteth", "eosusd", "eosbtc", "eoseth", "sanusd", "sanbtc", "saneth", "omgusd", "omgbtc", "omgeth", "bchusd", "bchbtc", "bcheth", "neousd", "neobtc", "neoeth", "etpusd", "etpbtc", "etpeth", "qtmusd", "qtmbtc", "qtmeth", "avtusd", "avtbtc", "avteth", "edousd", "edobtc", "edoeth", "btgusd", "btgbtc", "datusd", "datbtc", "dateth", "qshusd", "qshbtc", "qsheth", "yywusd", "yywbtc", "yyweth", "gntusd", "gntbtc", "gnteth", "sntusd", "sntbtc", "snteth", "ioteur", "batusd", "batbtc", "bateth", "mnausd", "mnabtc", "mnaeth", "funusd", "funbtc", "funeth", "zrxusd", "zrxbtc", "zrxeth", "tnbusd", "tnbbtc", "tnbeth", "spkusd", "spkbtc", "spketh", "trxusd", "trxbtc", "trxeth", "rcnusd", "rcnbtc", "rcneth", "rlcusd", "rlcbtc", "rlceth", "aidusd", "aidbtc", "aideth", "sngusd", "sngbtc", "sngeth", "repusd", "repbtc", "repeth", "elfusd", "elfbtc", "elfeth", "btcgbp", "etheur", "ethjpy", "ethgbp", "neoeur", "neojpy", "neogbp", "eoseur", "eosjpy", "eosgbp", "iotjpy", "iotgbp", "iosusd", "iosbtc", "ioseth", "aiousd", "aiobtc", "aioeth", "requsd", "reqbtc", "reqeth", "rdnusd", "rdnbtc", "rdneth", "lrcusd", "lrcbtc", "lrceth", "waxusd", "waxbtc", "waxeth", "daiusd", "daibtc", "daieth", "cfiusd", "cfibtc", "cfieth", "agiusd", "agibtc", "agieth", "bftusd", "bftbtc", "bfteth", "mtnusd", "mtnbtc", "mtneth", "odeusd", "odebtc", "odeeth", "antusd", "antbtc", "anteth", "dthusd", "dthbtc", "dtheth", "mitusd", "mitbtc", "miteth", "stjusd", "stjbtc", "stjeth", "xlmusd", "xlmeur", "xlmjpy", "xlmgbp", "xlmbtc", "xlmeth", "xvgusd", "xvgeur", "xvgjpy", "xvggbp", "xvgbtc", "xvgeth", "bciusd", "bcibtc", "mkrusd", "mkrbtc", "mkreth", "venusd", "venbtc", "veneth", "kncusd", "kncbtc", "knceth", "poausd", "poabtc", "poaeth" };


            foreach (var symbol in symbols)
            {
                foreach (var interval in intervals)
                {
                    await Console.Out.WriteLineAsync($"start:{symbol} {interval}");
                    await GetData(symbol, (KlineInterval)interval);
                    await Console.Out.WriteLineAsync($"end:{symbol} {interval}");
                }
            }
        }

        private static async Task GetData(string symbol,KlineInterval interval)
        {
            using (IDbConnection con = new MySqlConnection(conStr))
            {
                bool flag = true;
                var dateTime = new DateTime(2014, 1, 1);

                while (flag)
                {
                    try
                    {
                        Thread.Sleep(5000);

                        BitfinexMethod bitfinexMethod = new BitfinexMethod();

                        await Console.Out.WriteLineAsync($"Request symbol:{symbol} interval:{interval} start:{dateTime} end:{now}");
                        var result = await bitfinexMethod.GetHistoryKlines(symbol, interval, dateTime, now);
                        result.Sort((x, y) => x.timestamp.CompareTo(y.timestamp));
                        await Console.Out.WriteLineAsync($"Result  symbol:{symbol} interval:{interval} start:{result.FirstOrDefault().timestamp} end:{result.LastOrDefault().timestamp} count:{result.Count}");

                        var re = await con.ExecuteAsync($@"insert into klinedata(symbol,timeframe,timestamp,open,close,high,low,volume) values 
                                                        ('{symbol}','{interval}',@timestamp,@open,@close,@high,@low,@volume)
                                                        ON DUPLICATE KEY UPDATE open=@open,close=@close,high=@high,low=@low,volume=@volume", result);

                        dateTime = result.LastOrDefault().timestamp;

                        if (result.Count <= 1)
                        {
                            flag = false;
                        }
                    }
                    catch (Exception e)
                    {
                        await Console.Out.WriteLineAsync($"Error:{e.Message}");
                        var err = JArray.Parse(e.Message);
                        if (err[1].ToString() == "11010")
                        {
                            Thread.Sleep(5000);
                        }
                    }
                }
            }
        }
    }
}
