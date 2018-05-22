using System;

using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Newtonsoft.Json.Linq;

namespace BitfinexAPI
{
    static class ConvertHelper
    {
        public static T DataConvert<T>(string data)
        {
            return JsonConvert.DeserializeObject<T>(data);
        }

        public static string ObtainEnumValue<T>(T data)
        {
            var info = JsonConvert.SerializeObject(data, new StringEnumConverter());
            return JsonConvert.DeserializeObject<string>(info);
        }
    }

    class V1TimeConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            throw new NotImplementedException();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            return DateTimeOffset.FromUnixTimeSeconds((long)decimal.Parse(reader.Value.ToString())).DateTime;
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            writer.WriteValue(new DateTimeOffset((DateTime)value).ToUnixTimeSeconds());
        }
    }

    class KlineConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            throw new NotImplementedException();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var o = JArray.Load(reader);

            return new KlineInfo()
            {
                timestamp = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(o[0])).DateTime,
                open = Convert.ToDecimal(o[1]),
                close = Convert.ToDecimal(o[2]),
                high = Convert.ToDecimal(o[3]),
                low = Convert.ToDecimal(o[4]),
                volume = Convert.ToDecimal(o[5]),
            };
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }

    class TradeConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            throw new NotImplementedException();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            var o = JArray.Load(reader);

            return new TradeInfo()
            {
                id = Convert.ToInt64(o[0]),
                timestamp = DateTimeOffset.FromUnixTimeMilliseconds(Convert.ToInt64(o[1])).DateTime,
                amount = Convert.ToDecimal(o[2]),
                price = Convert.ToDecimal(o[3]),
            };
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }
    }
}
