using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

using Vit.Core.Module.Serialization;
using Vit.Extensions;
using Vit.Extensions.Serialize_Extensions;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext : Vitorm.DbContext
    {


        #region #1.0 Schema : Get Schema
        public virtual async Task<string> GetMappingAsync<Entity>()
        {
            var indexName = GetIndex<Entity>();
            return await GetMappingAsync(indexName);
        }

        public virtual async Task<string> GetMappingAsync(string indexName, bool throwErrorIfFailed = false)
        {
            var searchUrl = $"{readOnlyServerAddress}/{indexName}/_mapping";

            using var httpResponse = await httpClient.GetAsync(searchUrl);
            var strResponse = await httpResponse.Content.ReadAsStringAsync();

            if (throwErrorIfFailed && !httpResponse.IsSuccessStatusCode) throw new Exception(strResponse);
            return strResponse;
        }
        #endregion

        #region #1.1 Schema :  TryCreateTable

        public override async Task TryCreateTableAsync<Entity>()
        {
            var indexName = GetIndex<Entity>();
            await TryCreateTableAsync<Entity>(indexName);
        }
        public virtual async Task<string> TryCreateTableAsync<Entity>(string indexName, bool throwErrorIfFailed = false)
        {
            var url = $"{serverAddress}/{indexName}";

            var strMapping = BuildMapping<Entity>();
            var content = new StringContent(strMapping, Encoding.UTF8, "application/json");

            using var httpResponse = await httpClient.PutAsync(url, content);
            var strResponse = await httpResponse.Content.ReadAsStringAsync();

            if (throwErrorIfFailed && !httpResponse.IsSuccessStatusCode) throw new Exception(strResponse);
            return strResponse;
        }
        public string dateFormat = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis";
        public virtual string BuildMapping<Entity>()
        {
            /*
{
    "mappings":{
        "properties":{
            "birthday":{ "type":"date", "format":"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||strict_date_optional_time||epoch_millis" }
        }
    }
}
{
    "mappings":{
        "properties":{
            "@timestamp":{
                "type":"date"
            },
            "time":{
                "type":"date"
            }
        }
    }
}
             */
            if (typeof(Entity) == typeof(object))
            {
                //var strMapping = "{\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"time\":{\"type\":\"date\"}}}}";
                return "{\"mappings\":{\"properties\":{}}}";
            }
            var properties = new Dictionary<string, object>();
            var mapping = new { mappings = new { properties } };

            AddProperties(typeof(Entity).GetProperties(BindingFlags.Public | BindingFlags.Instance), Array.Empty<Type>());

            #region Add properties
            void AddProperties(PropertyInfo[] propertyInfos, IEnumerable<Type> typeCache, string parentPath = null)
            {
                if (propertyInfos?.Any() != true) return;

                foreach (PropertyInfo propertyInfo in propertyInfos)
                {
                    if (propertyInfo.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.NotMappedAttribute>() != null)
                        continue;

                    var columnAttr = propertyInfo.GetCustomAttribute<System.ComponentModel.DataAnnotations.Schema.ColumnAttribute>();
                    var columnName = columnAttr?.Name ?? propertyInfo.Name;
                    var databaseType = columnAttr?.TypeName;
                    var fieldPath = parentPath + columnName;

                    var propertyType = propertyInfo.PropertyType;

                    // Array or List
                    if (propertyType.IsArray) propertyType = propertyType.GetElementType();
                    else if (propertyType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(propertyType))
                    {
                        //  IEnumerable<T>  or  IQueryable<T>
                        propertyType = propertyType.GetGenericArguments()[0];
                    }

                    if (!propertyType.TypeIsValueTypeOrStringType() && !typeCache.Contains(propertyType))
                    {
                        var newTypeCache = typeCache.Concat(new[] { propertyType });
                        AddProperties(propertyType.GetProperties(BindingFlags.Public | BindingFlags.Instance), newTypeCache, fieldPath + ".");
                    }


                    if (!string.IsNullOrEmpty(databaseType))
                    {
                        properties[fieldPath] = Json.Deserialize<object>(databaseType);
                    }
                    else if (propertyType == typeof(DateTime) || propertyType == typeof(DateTime?))
                    {
                        properties[fieldPath] = new { type = "date", format = dateFormat };
                    }
                }
            }
            #endregion

            return Json.Serialize(mapping);
        }
        #endregion

        #region #1.2 Schema :  TryDropTable
        public override async Task TryDropTableAsync<Entity>()
        {
            var indexName = GetIndex<Entity>();
            await TryDropTableAsync(indexName);
        }

        public virtual async Task TryDropTableAsync(string indexName)
        {
            var url = $"{serverAddress}/{indexName}";
            using var httpResponse = await httpClient.DeleteAsync(url);

            if (httpResponse.IsSuccessStatusCode) return;

            var strResponse = await httpResponse.Content.ReadAsStringAsync();
            if (httpResponse.StatusCode == HttpStatusCode.NotFound && !string.IsNullOrWhiteSpace(strResponse)) return;

            throw new Exception(strResponse);
        }
        #endregion


        #region #1.1 Create :  Add

        public override async Task<Entity> AddAsync<Entity>(Entity entity)
        {
            var indexName = GetIndex<Entity>();
            return await AddAsync(entity, indexName);
        }
        public virtual async Task<Entity> AddAsync<Entity>(Entity entity, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var _id = GetDocumentId(entityDescriptor, entity);

            if (String.IsNullOrEmpty(_id))
            {
                return await SingleActionAsync(entityDescriptor, entity, indexName, "_doc");
            }
            else
            {
                return await SingleActionAsync(entityDescriptor, entity, indexName, "_create");
            }
        }

        #endregion


        #region #1.2 Create :  AddRange
        public override async Task AddRangeAsync<Entity>(IEnumerable<Entity> entities)
        {
            var indexName = GetIndex<Entity>();
            await AddRangeAsync(entities, indexName);
        }
        public virtual async Task AddRangeAsync<Entity>(IEnumerable<Entity> entities, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));
            var bulkResult = await BulkAsync(entityDescriptor, entities, indexName, "create");

            if (bulkResult.errors == true)
            {
                var reason = bulkResult.items?.FirstOrDefault(m => m.result?.error?.reason != null)?.result?.error?.reason;
                ThrowException(reason, bulkResult.responseBody);
            }

            var items = bulkResult?.items;
            if (items?.Length == entities.Count())
            {
                var t = 0;
                foreach (var entity in entities)
                {
                    SetKey(entityDescriptor, entity, items[t].result?._id);
                    t++;
                }
            }
        }
        #endregion




        #region #2.1 Retrieve : Get

        public override async Task<Entity> GetAsync<Entity>(object keyValue)
        {
            var indexName = GetIndex<Entity>();
            return await GetAsync<Entity>(keyValue, indexName);
        }
        public virtual async Task<Entity> GetAsync<Entity>(object keyValue, string indexName)
        {
            var actionUrl = $"{readOnlyServerAddress}/{indexName}/_doc/" + keyValue;

            using var httpResponse = await httpClient.GetAsync(actionUrl);

            if (httpResponse.StatusCode == HttpStatusCode.NotFound)
            {
                return default;
            }

            httpResponse.EnsureSuccessStatusCode();

            var strResponse = await httpResponse.Content.ReadAsStringAsync();
            var response = Deserialize<GetResult<Entity>>(strResponse);

            if (response.found != true) return default;

            var entity = response._source;

            if (entity != null && response._id != null)
            {
                var entityDescriptor = GetEntityDescriptor(typeof(Entity));
                SetKey(entityDescriptor, entity, response._id);
            }
            return entity;
        }

        /// <summary>
        /// result for   GET dev-orm/_doc/3
        /// </summary>
        /// <typeparam name="Entity"></typeparam>
        class GetResult<Entity>
        {
            public string _index { get; set; }
            public string _id { get; set; }

            public string _type { get; set; }
            public int? _version { get; set; }

            public int? _seq_no { get; set; }
            public int? _primary_term { get; set; }
            public bool? found { get; set; }
            public Entity _source { get; set; }
        }
        #endregion


        #region #3 Update: Update UpdateRange
        public override async Task<int> UpdateAsync<Entity>(Entity entity)
        {
            return await UpdateRangeAsync<Entity>(new[] { entity });
        }

        public virtual async Task<int> UpdateAsync<Entity>(Entity entity, string indexName)
        {
            return await UpdateRangeAsync<Entity>(new[] { entity }, indexName);
        }


        public override async Task<int> UpdateRangeAsync<Entity>(IEnumerable<Entity> entities)
        {
            var indexName = GetIndex<Entity>();
            return await UpdateRangeAsync<Entity>(entities, indexName);
        }

        public virtual async Task<int> UpdateRangeAsync<Entity>(IEnumerable<Entity> entities, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));
            if (entities.Any(entity => string.IsNullOrWhiteSpace(GetDocumentId(entityDescriptor, entity)))) throw new ArgumentNullException("_id");

            var bulkResult = await BulkAsync(entityDescriptor, entities, indexName, "update");

            if (bulkResult.items.Any() != true) ThrowException(bulkResult.responseBody);

            var rowCount = bulkResult.items.Count(item => item.update?.status == 200);

            return rowCount;
        }

        #endregion



        #region #4 Save SaveRange
        public virtual async Task<int> SaveAsync<Entity>(Entity entity)
        {
            var indexName = GetIndex<Entity>();
            return await SaveAsync<Entity>(entity, indexName);
        }

        public virtual async Task<int> SaveAsync<Entity>(Entity entity, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            entity = await SingleActionAsync(entityDescriptor, entity, indexName, "_doc");
            return entity != null ? 1 : 0;
        }

        public virtual async Task SaveRangeAsync<Entity>(IEnumerable<Entity> entities)
        {
            var indexName = GetIndex<Entity>();
            await SaveRangeAsync<Entity>(entities, indexName);
        }

        public virtual async Task SaveRangeAsync<Entity>(IEnumerable<Entity> entities, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));
            var bulkResult = await BulkAsync(entityDescriptor, entities, indexName, "index");

            if (bulkResult.errors == true)
            {
                var reason = bulkResult.items?.FirstOrDefault(m => m.result?.error?.reason != null)?.result?.error?.reason;
                ThrowException(reason, bulkResult.responseBody);
            }

            var items = bulkResult?.items;
            if (items?.Length == entities.Count())
            {
                var t = 0;
                foreach (var entity in entities)
                {
                    SetKey(entityDescriptor, entity, items[t].result?._id);
                    t++;
                }
            }
        }
        #endregion



        #region #5 Delete : Delete DeleteRange DeleteByKey DeleteByKeys

        public override async Task<int> DeleteAsync<Entity>(Entity entity)
        {
            var indexName = GetIndex<Entity>();
            return await DeleteAsync<Entity>(entity, indexName);
        }
        public virtual async Task<int> DeleteAsync<Entity>(Entity entity, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var key = GetDocumentId(entityDescriptor, entity);
            return await DeleteByKeyAsync(key, indexName);
        }



        public override async Task<int> DeleteRangeAsync<Entity>(IEnumerable<Entity> entities)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var keys = entities.Select(entity => GetDocumentId(entityDescriptor, entity)).ToList();
            return await DeleteByKeysAsync<Entity, object>(keys);
        }
        public virtual async Task<int> DeleteRangeAsync<Entity>(IEnumerable<Entity> entities, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var keys = entities.Select(entity => GetDocumentId(entityDescriptor, entity)).ToList();
            return await DeleteByKeysAsync<Entity, object>(keys, indexName);
        }


        public override async Task<int> DeleteByKeyAsync<Entity>(object keyValue)
        {
            var indexName = GetIndex<Entity>();
            return await DeleteByKeyAsync(keyValue, indexName);
        }
        public virtual async Task<int> DeleteByKeyAsync(object keyValue, string indexName)
        {
            var _id = keyValue?.ToString();

            if (string.IsNullOrWhiteSpace(_id)) throw new ArgumentNullException("_id");

            var actionUrl = $"{serverAddress}/{indexName}/_doc/" + _id;

            using var httpResponse = await httpClient.DeleteAsync(actionUrl);
            return httpResponse.IsSuccessStatusCode ? 1 : 0;

            //var strResponse = httpResponse.Content.ReadAsStringAsync().Result;
            /*
            {
              "_index": "user",
              "_type": "_doc",
              "_id": "5",
              "_version": 2,
              "result": "deleted",
              "_shards": {
                "total": 2,
                "successful": 1,
                "failed": 0
              },
              "_seq_no": 6,
              "_primary_term": 1
            }
            */
        }



        public override async Task<int> DeleteByKeysAsync<Entity, Key>(IEnumerable<Key> keys)
        {
            var indexName = GetIndex<Entity>();
            return await DeleteByKeysAsync<Entity, Key>(keys, indexName);
        }
        public virtual async Task<int> DeleteByKeysAsync<Entity, Key>(IEnumerable<Key> keys, string indexName)
        {
            var payload = new StringBuilder();
            foreach (var _id in keys)
            {
                payload.AppendLine($"{{\"delete\":{{\"_index\":\"{indexName}\",\"_id\":\"{_id}\"}}}}");
            }
            var actionUrl = $"{serverAddress}/{indexName}/_bulk";
            using var content = new StringContent(payload.ToString(), Encoding.UTF8, "application/json");
            using var httpResponse = await httpClient.PostAsync(actionUrl, content);

            var strResponse = await httpResponse.Content.ReadAsStringAsync();
            if (string.IsNullOrWhiteSpace(strResponse)) httpResponse.EnsureSuccessStatusCode();

            var response = Deserialize<BulkResponse>(strResponse);

            if (response.errors == true)
            {
                var reason = response.items?.FirstOrDefault(m => m.result?.error?.reason != null)?.result?.error?.reason;
                ThrowException(reason, strResponse);
            }

            return response.items.Count(item => item.result?.status == 200);
        }



        #endregion

    }
}
