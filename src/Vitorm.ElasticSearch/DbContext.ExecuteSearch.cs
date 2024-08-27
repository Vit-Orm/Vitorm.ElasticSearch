using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Vitorm.ElasticSearch.QueryBuilder;
using Vitorm.ElasticSearch.QueryExecutor;
using Vitorm.ElasticSearch.SearchExecutor;
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


        #region SearchExecutor
        public static List<ISearchExecutor> defaultSearchExecutor = new() { new PlainSearchExecutor() };
        public List<ISearchExecutor> searchExecutor = defaultSearchExecutor;
        #endregion


        public virtual async Task<bool> ExecuteSearchAsync<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            foreach (var executor in searchExecutor)
            {
                var success = await executor.ExecuteSearchAsync<Entity, ResultEntity>(arg);
                if (success) return true;
            }
            throw new NotSupportedException("not supported Search");
        }




        public virtual async Task<(IEnumerable<Entity> entities, int? totalCount)> QueryAsync<Entity>(object queryPayload, string indexName)
        {
            var searchResult = await ExecuteSearchAsync<QueryResponse<Entity>>(queryPayload, indexName: indexName);

            var entityDescriptor = GetEntityDescriptor(typeof(Entity));
            var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(this, entityDescriptor));
            var totalCount = searchResult?.hits?.total?.value;
            return (entities, totalCount);
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
