using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Vit.Linq;

using Vitorm.Entity;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext : Vitorm.DbContext
    {
        static void ThrowException(params string[] messages)
        {
            Exception ex = null;
            messages.Reverse().ForEach(msg => ex = new Exception(msg, ex));
            throw ex;
        }


        public class Shards
        {
            public int? total { get; set; }
            public int? successful { get; set; }
            public int? failed { get; set; }
        }
        public class PlainError
        {
            public string type { get; set; }
            public string reason { get; set; }
        }


        protected virtual async Task<HttpResponseMessage> SendHttpRequestAsync(string url, HttpMethod httpMethod, object payload = null)
        {
            this.Event_OnExecuting(executeString: url, param: payload);

            string strPayload = payload as string;
            if (strPayload == null && payload is not null) strPayload = Serialize(payload);

            using var content = strPayload == null ? null : new StringContent(strPayload, Encoding.UTF8, "application/json");
            using var httpRequestMessage = new HttpRequestMessage(httpMethod ?? HttpMethod.Get, url) { Content = content };
            return await httpClient.SendAsync(httpRequestMessage);
        }




        #region ExecuteActionAsync
        protected virtual async Task<Entity> ExecuteActionAsync<Entity>(IEntityDescriptor entityDescriptor, Entity entity, string indexName, string action, HttpMethod httpMethod = null)
        {
            var _id = GetDocumentId(entityDescriptor, entity);

            string actionUrl = $"{serverAddress}/{indexName}/{action}/{_id}";

            using HttpResponseMessage response = await SendHttpRequestAsync(actionUrl, httpMethod ?? HttpMethod.Post, entity);

            var strResponse = await response.Content.ReadAsStringAsync();

            var result = Deserialize<ActionResult>(strResponse);

            if (result.error?.reason != null)
            {
                ThrowException(result.error.reason, strResponse);
            }
            response.EnsureSuccessStatusCode();

            SetKey(entityDescriptor, entity, result._id);

            return entity;
        }

        protected virtual void SetKey<Entity>(IEntityDescriptor entityDescriptor, Entity entity, string _id)
        {
            if (entity == null || entityDescriptor?.key == null) return;

            var keyType = TypeUtil.GetUnderlyingType(entityDescriptor.key.type);
            var newKeyValue = TypeUtil.ConvertToUnderlyingType(_id, keyType);
            if (newKeyValue != null)
            {
                entityDescriptor.key.SetValue(entity, newKeyValue);
            }
        }

        class ActionResult
        {
            public string _index { get; set; }
            public string _type { get; set; }
            public string _id { get; set; }
            public int? _version { get; set; }
            public string result { get; set; }

            public int? _seq_no { get; set; }
            public int? _primary_term { get; set; }

            public Shards _shards { get; set; }

            #region error
            public PlainError error { get; set; }
            public int? status { get; set; }

            /*
             {
  "error": {
    "root_cause": [
      {
        "type": "version_conflict_engine_exception",
        "reason": "[WsjwFpABUFeGD8Mmz4Kv]: version conflict, document already exists (current version [1])",
        "index_uuid": "-18uwvn_R8KGGP2h10VG8w",
        "shard": "0",
        "index": "user"
      }
    ],
    "type": "version_conflict_engine_exception",
    "reason": "[WsjwFpABUFeGD8Mmz4Kv]: version conflict, document already exists (current version [1])",
    "index_uuid": "-18uwvn_R8KGGP2h10VG8w",
    "shard": "0",
    "index": "user"
  },
  "status": 409
}
             */
            #endregion
        }
        #endregion


        #region ExecuteBulkActionAsync


        /// <summary>
        /// action: create | index | update | delete
        /// </summary>
        /// <typeparam name="Entity"></typeparam>
        /// <param name="entities"></param>
        /// <param name="indexName"></param>
        /// <param name="action"></param>
        protected async Task<BulkResponse> ExecuteBulkActionAsync<Entity>(IEntityDescriptor entityDescriptor, IEnumerable<Entity> entities, string indexName, string action)
        {
            var payload = new StringBuilder();

            if (action == "update")
            {
                foreach (var entity in entities)
                {
                    var docId = GetDocumentId(entityDescriptor, entity);
                    if (string.IsNullOrEmpty(docId))
                    {
                        throw new ArgumentException("key could not be null in BulkUpdate");
                    }
                    payload.AppendLine($"{{\"{action}\":{{\"_index\":\"{indexName}\",\"_id\":\"{docId}\"}}}}");
                    payload.Append("{\"doc\":").Append(Serialize(entity)).AppendLine("}");
                }
            }
            else if (action == "delete")
            {
                foreach (var entity in entities)
                {
                    var docId = GetDocumentId(entityDescriptor, entity);
                    if (string.IsNullOrEmpty(docId))
                    {
                        throw new ArgumentException("key could not be null in BulkDelete");
                    }
                    payload.AppendLine($"{{\"{action}\":{{\"_index\":\"{indexName}\",\"_id\":\"{docId}\"}}}}");
                }
            }
            else
            {
                // index
                foreach (var entity in entities)
                {
                    var docId = GetDocumentId(entityDescriptor, entity);
                    if (string.IsNullOrEmpty(docId))
                    {
                        payload.AppendLine($"{{\"{action}\":{{\"_index\":\"{indexName}\"}}}}");
                    }
                    else
                    {
                        payload.AppendLine($"{{\"{action}\":{{\"_index\":\"{indexName}\",\"_id\":\"{GetDocumentId(entityDescriptor, entity)}\"}}}}");
                    }
                    payload.AppendLine(Serialize(entity));
                }
            }
            var actionUrl = $"{serverAddress}/{indexName}/_bulk";

            using var httpResponse = await SendHttpRequestAsync(actionUrl, HttpMethod.Post, payload.ToString());

            var strResponse = await httpResponse.Content.ReadAsStringAsync();
            if (string.IsNullOrWhiteSpace(strResponse)) httpResponse.EnsureSuccessStatusCode();

            var response = Deserialize<BulkResponse>(strResponse);
            response.responseBody = strResponse;

            return response;
        }

        public class BulkResponse
        {
            public string responseBody { get; set; }
            public int? took { get; set; }
            public bool? errors { get; set; }
            public Item[] items { get; set; }

            public class Item
            {
                public ItemStatus index { get; set; }
                public ItemStatus create { get; set; }
                public ItemStatus update { get; set; }
                public ItemStatus delete { get; set; }
                public ItemStatus result => index ?? create ?? update ?? delete;
            }
            public class ItemStatus
            {
                public string _id { get; set; }
                public string _index { get; set; }
                public string _type { get; set; }
                public int? _version { get; set; }
                public string result { get; set; }
                public int? _seq_no { get; set; }
                public int? _primary_term { get; set; }
                public int? status { get; set; }

                public Shards _shards { get; set; }
                public PlainError error { get; set; }

            }
        }

        #endregion



        #region ExecuteSearch
        public virtual async Task<Result> ExecuteSearchAsync<Result>(object queryPayload, string searchUrl = null, string indexName = null)
        {
            searchUrl ??= $"{readOnlyServerAddress}/{indexName}/_search";

            using var httpResponse = await SendHttpRequestAsync(searchUrl, HttpMethod.Post, queryPayload);

            var strResponse = await httpResponse.Content.ReadAsStringAsync();
            if (!httpResponse.IsSuccessStatusCode) throw new Exception(strResponse);

            return Deserialize<Result>(strResponse);
        }
        #endregion
    }
}
