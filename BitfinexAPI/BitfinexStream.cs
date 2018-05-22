using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace BitfinexAPI
{
    public class BitfinexStream
    {
        string _symbol;

        public BitfinexStream(string symbol)
        {
            _symbol = symbol.ToLower();
        }

        public async Task RetrieveKline(Action<string> handler, string symbol, KlineInterval interval)
        {
            //if (_klineSocketId.HasValue)
            //    AbortKline();
            string payload = "{\"event\": \"subscribe\",\"channel\": \"candles\",\"key\": \"trade:"
                            + ConvertHelper.ObtainEnumValue(interval) + ":t" + symbol + "\"}";
            //Payload payload = new Payload()
            //{
            //    Channel = "candles",
            //    Key = $"trade:{ConvertHelper.ObtainEnumValue(interval)}:t{symbol}"
            //};

            int _klineSocketId = await AccessWebSocket.Subscribe(payload, handler);
        }

        public async Task RetrieveKline2(Action<string> handler,List<string> lst) //string symbol, KlineInterval interval)
        {
            var payloadlst = lst.Select((code) => "{\"event\": \"subscribe\",\"channel\": \"candles\",\"key\": \"trade:" + ConvertHelper.ObtainEnumValue(KlineInterval.OneMinute) + ":t" + code.Split('.')[0] + "\"}");


            int _klineSocketId = await AccessWebSocket.Subscribe2(payloadlst, handler);
        }
    }

    //public class Payload
    //{
    //    public string Event => "subscribe";
    //    public string Channel { get; set; }
    //    public string Key { get; set; }
    //}
}
