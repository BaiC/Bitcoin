using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;
using WebSocketSharp;

namespace BitfinexAPI
{
    static class AccessWebSocket
    {
        const string endpointBase = "wss://api.bitfinex.com/ws/2";

        static Dictionary<int, WebSocket> _socketPool;
        static int _socketIdCounter;

        static AccessWebSocket()
        {
            _socketPool = new Dictionary<int, WebSocket>();
            _socketIdCounter = 0;
        }

        public static async Task<int> Subscribe(string args, Action<string> handler)
        {
            WebSocket ws = new WebSocket(endpointBase);
            ws.SetProxy("http://localhost:1080", null, null);

            ws.OnMessage += (sender, message) =>
            {
                //handler(ConvertHelper.DataConvert<T>(message.Data));
                handler(message.Data);
            };

            ws.OnError += (sender, error) =>
            {
                throw new Exception("WebSocketException:" + error.Message);
            };

            ws.OnOpen += (sender, e) =>
            {
                ws.Send(args);
            };

            ws.ConnectAsync();

            _socketPool.Add(++_socketIdCounter, ws);

            return _socketIdCounter;
        }

        public static async Task<int> Subscribe2(IEnumerable<string> args, Action<string> handler)
        {
            WebSocket ws = new WebSocket(endpointBase);
            ws.SetProxy("http://localhost:1080", null, null);

            ws.OnMessage += (sender, message) =>
            {
                //handler(ConvertHelper.DataConvert<T>(message.Data));
                handler(message.Data);
            };

            ws.OnError += (sender, error) =>
            {
                throw new Exception("WebSocketException:" + error.Message);
            };

            ws.OnOpen += (sender, e) =>
            {
                foreach (var item in args)
                {
                    Console.WriteLine(item);
                    ws.Send(item);
                }
            };

           ws.ConnectAsync();

            _socketPool.Add(++_socketIdCounter, ws);

            return _socketIdCounter;
        }

        public static void Unsubscribe(int socketId)
        {
            _socketPool[socketId].Close();
            _socketPool.Remove(socketId);
        }
    }
}
