using System.Runtime.Serialization;

namespace BitfinexAPI
{
    public enum OrderSide
    {
        [EnumMember(Value = "buy")]
        BUY,
        [EnumMember(Value = "sell")]
        SELL,
    }

    public enum OrderType
    {
        [EnumMember(Value = "limit")]
        LIMIT,
        [EnumMember(Value = "market")]
        MARKET,
        [EnumMember(Value = "exchange limit")]
        EXCHANGE_LIMIT,
        [EnumMember(Value = "exchange market")]
        EXCHANGE_MARKET,
    }

    public enum WalletType
    {
        [EnumMember(Value = "exchange")]
        EXCHANGE,
        [EnumMember(Value = "trading")]
        TRADING,
        [EnumMember(Value = "funding")]
        FUNDING,
    }

    public enum KlineInterval
    {
        [EnumMember(Value = "1m")]
        OneMinute,
        [EnumMember(Value = "5m")]
        FiveMinutes,
        [EnumMember(Value = "15m")]
        FifteenMinutes,
        [EnumMember(Value = "30m")]
        ThirtyMinutes,
        [EnumMember(Value = "1h")]
        OneHour,
        [EnumMember(Value = "3h")]
        ThreeHours,
        [EnumMember(Value = "6h")]
        SixHours,
        [EnumMember(Value = "12h")]
        TwelveHours,
        [EnumMember(Value = "1D")]
        OneDay,
        [EnumMember(Value = "7D")]
        OneWeek,
        [EnumMember(Value = "14D")]
        TwoWeeks,
        [EnumMember(Value = "1M")]
        OneMonth,
    }
}
