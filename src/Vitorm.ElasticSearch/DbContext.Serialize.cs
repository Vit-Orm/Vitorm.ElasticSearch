using System.Reflection;

using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

using Vit.Core.Module.Serialization;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext : Vitorm.DbContext
    {
        // Serialize
        public virtual string Serialize<Model>(Model m)
        {
            return JsonConvert.SerializeObject(m, serializeSetting);
        }
        public static readonly JsonSerializerSettings defaultSerializeSetting = new()
        {
            NullValueHandling = NullValueHandling.Include,
            DateFormatHandling = DateFormatHandling.IsoDateFormat,
            DateFormatString = "yyyy-MM-dd HH:mm:ss",
            ContractResolver = new SerializeContractResolver()
        };
        public JsonSerializerSettings serializeSetting { get; set; } = defaultSerializeSetting;
        public virtual Model Deserialize<Model>(string jsonString)
        {
            return Json.Deserialize<Model>(jsonString);
        }



        public class SerializeContractResolver : DefaultContractResolver
        {
            protected override JsonProperty CreateProperty(MemberInfo member, MemberSerialization memberSerialization)
            {
                var property = base.CreateProperty(member, memberSerialization);
                var attr = member.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>();
                if (!string.IsNullOrWhiteSpace(attr?.Name))
                {
                    property.PropertyName = attr.Name;
                }
                return property;
            }
        }


    }
}
