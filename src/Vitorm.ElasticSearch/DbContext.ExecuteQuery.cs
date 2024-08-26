using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Vitorm.ElasticSearch.QueryBuilder;
using Vitorm.Entity;
using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext : Vitorm.DbContext
    {
        /// <summary>
        /// default is 10000.
        /// {"type":"illegal_argument_exception","reason":"Result window is too large, from + size must be less than or equal to: [10000] but was [10001]. See the scroll api for a more efficient way to request large data sets. This limit can be set by changing the [index.max_result_window] index level setting."}
        /// </summary>
        public int maxResultWindowSize = 10000;
        /// <summary>
        /// https://www.elastic.co/guide/en/elasticsearch/reference/7.0/search-request-track-total-hits.html
        /// </summary>
        public bool track_total_hits = false;




        public virtual QueryResponse<Entity> Query<Entity>(object query, string indexName)
        {
            return QueryAsync<Entity>(query, indexName).Result;
        }

        public virtual async Task<Result> InvokeQueryAsync<Result>(object queryPayload, string searchUrl = null, string indexName = null)
        {
            if (queryPayload is not string strQuery) strQuery = Serialize(queryPayload);

            searchUrl ??= $"{readOnlyServerAddress}/{indexName}/_search";

            using var searchContent = new StringContent(strQuery, Encoding.UTF8, "application/json");
            using var httpResponse = await httpClient.PostAsync(searchUrl, searchContent);

            var strResponse = await httpResponse.Content.ReadAsStringAsync();
            if (!httpResponse.IsSuccessStatusCode) throw new Exception(strResponse);

            return Deserialize<Result>(strResponse);
        }

        public virtual async Task<QueryResponse<Entity>> QueryAsync<Entity>(object queryPayload, string indexName)
        {
            return await InvokeQueryAsync<QueryResponse<Entity>>(queryPayload, indexName: indexName);
        }


        private static ExpressionNodeBuilder defaultExpressionNodeBuilder_;
        public static ExpressionNodeBuilder defaultExpressionNodeBuilder
        {
            get => defaultExpressionNodeBuilder_ ?? (defaultExpressionNodeBuilder_ = new());
            set => defaultExpressionNodeBuilder_ = value;
        }

        public ExpressionNodeBuilder expressionNodeBuilder = defaultExpressionNodeBuilder;

        public virtual Dictionary<string, object> ConvertStreamToQueryPayload(CombinedStream combinedStream)
        {
            var queryPayload = new Dictionary<string, object>();

            // #1 where
            queryPayload["query"] = expressionNodeBuilder.ConvertToQuery(combinedStream.where);

            // #2 orders
            if (combinedStream.orders?.Any() == true)
            {
                queryPayload["sort"] = combinedStream.orders
                    .Select(order =>
                    {
                        var field = expressionNodeBuilder.GetNodeField(order.member, out var fieldType);
                        if (fieldType == typeof(string)) field += ".keyword";
                        return new Dictionary<string, object> { [field] = new { order = order.asc ? "asc" : "desc" } };
                    })
                    .ToList();
            }

            // #3 skip take
            int skip = 0;
            if (combinedStream.skip > 0)
                queryPayload["from"] = skip = combinedStream.skip.Value;

            var take = combinedStream.take >= 0 ? combinedStream.take.Value : maxResultWindowSize;
            if (take + skip > maxResultWindowSize) take = maxResultWindowSize - skip;
            queryPayload["size"] = take;


            // #4 track_total_hits
            if (track_total_hits) queryPayload["track_total_hits"] = true;

            return queryPayload;
        }





        public class QueryResponse<T>
        {
            public string _scroll_id { get; set; }
            public HitsContainer hits { get; set; }
            public class HitsContainer
            {
                public List<Hit> hits { get; set; }
                public Total total { get; set; }
                public class Total
                {
                    public int? value { get; set; }
                }
                public class Hit
                {
                    public string _index { get; set; }
                    public string _type { get; set; }
                    public string _id { get; set; }
                    public float? _score { get; set; }
                    public T _source { get; set; }

                    public T GetSource(DbContext dbContext, IEntityDescriptor entityDescriptor)
                    {
                        if (_source != null && _id != null)
                            dbContext.SetKey(entityDescriptor, _source, _id);
                        return _source;
                    }
                }
            }
        }



    }
}
