using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace BitfinexAPI
{
    public class BitfinexMethod
    {
        string _apiKey;
        string _secretKey;

        public BitfinexMethod(string apiKey, string secretKey)
        {
            _apiKey = apiKey;
            _secretKey = secretKey;
        }

        BaseInfo GeneratePayload(string path)
        {
            var args = new BaseInfo();
            args.Add("request", path);
            args.Add("nonce", DateTime.Now.Ticks.ToString());

            return args;
        }

        async Task<T> ProcessPublic<T>(string path)
        {
            return await AccessRestApi.InvokeHttpCall<T>(path);
        }

        async Task<T> ProcessAuthenticated<T>(BaseInfo args)
        {
            return await AccessRestApi.InvokeHttpCall<T>(
                (string)args["request"], args, _apiKey, _secretKey);
        }

        public async Task<List<string>> GetSymbols()
        {
            return await ProcessPublic<List<string>>("/v1/symbols");
        }

        public async Task<List<TransactionInfo>> GetTrades(string symbol)
        {
            return await ProcessPublic<List<TransactionInfo>>("/v1/trades/" + symbol.ToLower());
        }

        public async Task<OrderBookInfo> GetOrderBook(string symbol)
        {
            return await ProcessPublic<OrderBookInfo>("/v1/book/" + symbol.ToLower());
        }

        public async Task<List<BalanceInfo>> GetBalances()
        {
            var args = GeneratePayload("/v1/balances");
            return await ProcessAuthenticated<List<BalanceInfo>>(args);
        }

        public async Task<List<OrderInfo>> GetActiveOrders()
        {
            var args = GeneratePayload("/v1/orders");
            return await ProcessAuthenticated<List<OrderInfo>>(args);
        }

        public async Task<List<OrderInfo>> GetOrdersHistory()
        {
            var args = GeneratePayload("/v1/orders/hist");
            return await ProcessAuthenticated<List<OrderInfo>>(args);
        }

        public async Task<List<PositionInfo>> GetActivePositions()
        {
            var args = GeneratePayload("/v1/positions");
            return await ProcessAuthenticated<List<PositionInfo>>(args);
        }

        public async Task<List<BaseInfo>> TransferWallets(
            decimal amount,
            string currency,
            WalletType walletfrom,
            WalletType walletto)
        {
            var args = GeneratePayload("/v1/transfer");
            args.Add("amount", amount.ToString());
            args.Add("currency", currency);
            args.Add("walletfrom", ConvertHelper.ObtainEnumValue(walletfrom));
            args.Add("walletto", ConvertHelper.ObtainEnumValue(walletto));

            return await ProcessAuthenticated<List<BaseInfo>>(args);
        }

        public async Task<OrderInfo> CreateOrder(
            string symbol,
            decimal amount,
            decimal price,
            OrderSide side,
            OrderType type)
        {
            var args = GeneratePayload("/v1/order/new");
            args.Add("exchange", "bitfinex");
            args.Add("symbol", symbol.ToUpper());
            args.Add("amount", amount.ToString());
            args.Add("price", price.ToString());
            args.Add("side", ConvertHelper.ObtainEnumValue(side));
            args.Add("type", ConvertHelper.ObtainEnumValue(type));

            return await ProcessAuthenticated<OrderInfo>(args);
        }

        public async Task<BaseInfo> CancelAllOrders()
        {
            var args = GeneratePayload("/v1/order/cancel/all");
            return await ProcessAuthenticated<BaseInfo>(args);
        }

        public async Task<BaseInfo> ClosePosition(long id)
        {
            var args = GeneratePayload("/v1/position/close");
            args.Add("position_id", id);

            return await ProcessAuthenticated<BaseInfo>(args);
        }

        public async Task<List<KlineInfo>> GetHistoryKlines(
            string symbol,
            KlineInterval interval,
            DateTime start,
            DateTime end,
            int limit = 800)
        {
            long s = new DateTimeOffset(start).ToUnixTimeMilliseconds();
            long e = new DateTimeOffset(end).ToUnixTimeMilliseconds();

            string path = "/v2/candles/trade:"
                + ConvertHelper.ObtainEnumValue(interval)
                + ":t" + symbol.ToUpper()
                + "/hist?limit=" + limit.ToString()
                + "&start=" + s.ToString()
                + "&end=" + e.ToString()
                + "&sort=1";

            return await ProcessPublic<List<KlineInfo>>(path);
        }

        public async Task<List<TradeInfo>> GetHistoryTrades(
            string symbol,
            DateTime start,
            DateTime end,
            int limit = 800)
        {
            long s = new DateTimeOffset(start).ToUnixTimeMilliseconds();
            long e = new DateTimeOffset(end).ToUnixTimeMilliseconds();

            string path = "/v2/trades/t" + symbol.ToUpper()
                + "/hist?limit=" + limit.ToString()
                + "&start=" + s.ToString()
                + "&end=" + e.ToString()
                + "&sort=1";

            return await ProcessPublic<List<TradeInfo>>(path);
        }
    }
}
