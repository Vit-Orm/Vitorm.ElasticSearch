using System;
using System.Net.Http;

using Newtonsoft.Json;

using Vit.Core.Module.Serialization;

using Vitorm.Entity;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext : Vitorm.DbContext
    {

        /// <summary>
        /// es address, example:"http://localhost:9200"
        /// </summary>
        public string serverAddress { get; set; }

        /// <summary>
        /// es address, example:"http://localhost:9200"
        /// </summary>
        protected string _readOnlyServerAddress { get; set; }


        /// <summary>
        /// es address, example:"http://localhost:9200"
        /// </summary>
        public string readOnlyServerAddress { get => _readOnlyServerAddress ?? serverAddress; set => _readOnlyServerAddress = value; }


        public System.Net.Http.HttpClient httpClient = null;
        public static System.Net.Http.HttpClient defaultHttpClient = null;

        public DbContext(string serverAddress, System.Net.Http.HttpClient httpClient = null, int? commandTimeout = null)
            : this(new DbConfig(connectionString: serverAddress, commandTimeout: commandTimeout), httpClient)
        {
        }

        public DbContext(DbConfig dbConfig, System.Net.Http.HttpClient httpClient = null)
        {
            this.serverAddress = dbConfig.connectionString;
            this._readOnlyServerAddress = dbConfig.readOnlyConnectionString;

            if (httpClient == null)
            {
                defaultHttpClient ??= CreateHttpClient();
                if (dbConfig.commandTimeout.HasValue && dbConfig.commandTimeout.Value != (int)defaultHttpClient.Timeout.TotalSeconds)
                    httpClient = CreateHttpClient(dbConfig.commandTimeout.Value);
                else
                    httpClient = defaultHttpClient;
            }
            this.httpClient = httpClient;

            if (!string.IsNullOrWhiteSpace(dbConfig.dateFormat)) this.dateFormat = dbConfig.dateFormat;
            if (dbConfig.maxResultWindowSize.HasValue) this.maxResultWindowSize = dbConfig.maxResultWindowSize.Value;
            if (dbConfig.track_total_hits.HasValue) this.track_total_hits = dbConfig.track_total_hits.Value;


            this.GetEntityIndex = GetDefaultIndex;

            dbGroupName = "ES_DbSet_" + GetHashCode();
        }

        HttpClient CreateHttpClient(int? commandTimeout = null)
        {
            // trust all certificate
            var HttpHandler = new HttpClientHandler
            {
                ServerCertificateCustomValidationCallback = (a, b, c, d) => true
            };
            var httpClient = new System.Net.Http.HttpClient(HttpHandler);
            if (commandTimeout.HasValue) httpClient.Timeout = TimeSpan.FromSeconds(commandTimeout.Value);

            return httpClient;
        }


        // GetIndex
        public virtual Func<Type, string> GetEntityIndex { set; get; }
        public virtual string GetIndex<Model>()
        {
            return GetEntityIndex(typeof(Model));
        }
        public string GetDefaultIndex(Type entityType)
        {
            var entityDescriptor = GetEntityDescriptor(entityType);
            return (entityDescriptor?.tableName ?? entityType.Name).ToLower();
        }


        // GetDocumentId
        public static Func<IEntityDescriptor, object, string> DefaultGetDocumentId = (IEntityDescriptor entityDescriptor, object entity) => entityDescriptor?.key?.GetValue(entity)?.ToString();
        public Func<IEntityDescriptor, object, string> GetDocumentId = DefaultGetDocumentId;

        public static string GetEntityId(IEntityDescriptor entityDescriptor, object entity)
        {
            var key = entityDescriptor?.key;
            object keyValue = null;
            if (entity is not null && key is not null)
            {
                keyValue = key.GetValue(entity);

                if (keyValue is null || keyValue.Equals(TypeUtil.DefaultValue(key.type)))
                    return null;
            }
            return keyValue?.ToString();
        }

        // Serialize
        public virtual string Serialize<Model>(Model m)
        {
            return JsonConvert.SerializeObject(m, serializeSetting);
        }
        public static readonly JsonSerializerSettings defaultSerializeSetting = new() { NullValueHandling = NullValueHandling.Include, DateFormatHandling = DateFormatHandling.IsoDateFormat, DateFormatString = "yyyy-MM-dd HH:mm:ss" };
        public JsonSerializerSettings serializeSetting { get; set; } = defaultSerializeSetting;
        public virtual Model Deserialize<Model>(string jsonString)
        {
            return Json.Deserialize<Model>(jsonString);
        }

    }
}