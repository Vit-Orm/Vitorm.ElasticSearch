using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;

using Vitorm.Entity;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext
    {
        static void ThrowException(string message, string plainResult) => throw new Exception(message, new Exception(plainResult));


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


        protected virtual Entity SingleAction<Entity>(IEntityDescriptor entityDescriptor, Entity entity, string indexName, string action)
        {
            var _id = entityDescriptor.key.GetValue(entity) as string;
            var actionUrl = $"{serverAddress}/{indexName}/{action}/{_id}";
            var content = new StringContent(Serialize(entity), Encoding.UTF8, "application/json");
            var response = httpClient.PostAsync(actionUrl, content).Result;
            var strResponse = response.Content.ReadAsStringAsync().Result;

            var result = Deserialize<AddResult>(strResponse);

            if (result.error?.reason != null)
            {
                ThrowException(result.error.reason, strResponse);
            }
            response.EnsureSuccessStatusCode();

            _id = result._id;
            if (_id != null) entityDescriptor.key?.SetValue(entity, _id);

            return entity;
        }
        class AddResult
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

        #region Bulk

        /// <summary>
        /// action: create | index | update | delete
        /// </summary>
        /// <typeparam name="Entity"></typeparam>
        /// <param name="entitys"></param>
        /// <param name="indexName"></param>
        /// <param name="action"></param>
        protected BulkResponse Bulk<Entity>(IEntityDescriptor entityDescriptor, IEnumerable<Entity> entitys, string indexName, string action)
        {
            var payload = new StringBuilder();
            foreach (var entity in entitys)
            {
                payload.AppendLine($"{{\"{action}\":{{\"_index\":\"{indexName}\",\"_id\":\"{GetDocumentId(entityDescriptor, entity)}\"}}}}");
                payload.AppendLine(Serialize(entity));
            }
            var actionUrl = $"{serverAddress}/{indexName}/_bulk";
            var content = new StringContent(payload.ToString(), Encoding.UTF8, "application/json");
            var httpResponse = httpClient.PostAsync(actionUrl, content).Result;

            var strResponse = httpResponse.Content.ReadAsStringAsync().Result;
            if (string.IsNullOrWhiteSpace(strResponse)) httpResponse.EnsureSuccessStatusCode();

            var response = Deserialize<BulkResponse>(strResponse);

            if (response.errors == true)
            {
                var reason = response.items?.FirstOrDefault(m => m.result?.error?.reason != null)?.result?.error?.reason;
                ThrowException(reason, strResponse);
            }

            return response;

        }
        public class BulkResponse
        {
            public int? took { get; set; }
            public bool? errors { get; set; }
            public Item[] items { get; set; }

            public class Item
            {
                public ItemStatus create { get; set; }
                public ItemStatus delete { get; set; }
                public ItemStatus result => create ?? delete;
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
    }
}
