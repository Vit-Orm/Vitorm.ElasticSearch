using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

using Vit.Linq.ExpressionTree.ComponentModel;

using Vitorm.Entity;
using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext
    {
        /// <summary>
        /// default is 10000.
        /// {"type":"illegal_argument_exception","reason":"Result window is too large, from + size must be less than or equal to: [10000] but was [10001]. See the scroll api for a more efficient way to request large data sets. This limit can be set by changing the [index.max_result_window] index level setting."}
        /// </summary>
        public int maxResultWindowSize = 10000;


        protected virtual Delegate BuildSelect(Type entityType, ExpressionNode selectedFields, string entityParameterName)
        {
            // Compile Lambda

            var lambdaNode = ExpressionNode.Lambda(new[] { entityParameterName }, selectedFields);
            //var strNode = Json.Serialize(lambdaNode);

            var lambdaExp = convertService.ConvertToCode_LambdaExpression(lambdaNode, new[] { entityType });
            return lambdaExp.Compile();
        }

        protected virtual QueryResponse<Model> Query<Model>(object query, string indexName)
        {
            return QueryAsync<Model>(query, indexName).Result;
        }
        public virtual string Query(string query, string indexName)
        {
            return QueryAsync(query, indexName).Result;
        }




        protected virtual async Task<QueryResponse<Model>> QueryAsync<Model>(object query, string indexName)
        {
            string strQuery = query == null ? null : (query as string) ?? Serialize(query);
            var strResponse = await QueryAsync(strQuery, indexName);
            return Deserialize<QueryResponse<Model>>(strResponse);
        }
        public virtual async Task<string> QueryAsync(string query, string indexName)
        {
            var searchUrl = $"{readOnlyServerAddress}/{indexName}/_search";

            using var searchContent = new StringContent(query, Encoding.UTF8, "application/json");
            using var httpResponse = await httpClient.PostAsync(searchUrl, searchContent);

            var strResponse = await httpResponse.Content.ReadAsStringAsync();
            if (!httpResponse.IsSuccessStatusCode) throw new Exception(strResponse);

            return strResponse;
        }

        private static ExpressionNodeBuilder defaultExpressionNodeBuilder_;
        public static ExpressionNodeBuilder defaultExpressionNodeBuilder
        {
            get => defaultExpressionNodeBuilder_ ?? (defaultExpressionNodeBuilder_ = new());
            set => defaultExpressionNodeBuilder_ = value;
        }

        public ExpressionNodeBuilder expressionNodeBuilder = defaultExpressionNodeBuilder;

        public virtual object ConvertStreamToQuery(CombinedStream combinedStream)
        {
            var queryBody = new Dictionary<string, object>();

            // #1 where
            queryBody["query"] = expressionNodeBuilder.ConvertToQuery(combinedStream.where);

            // #2 orders
            if (combinedStream.orders?.Any() == true)
            {
                queryBody["sort"] = combinedStream.orders
                                 .Select(order => new Dictionary<string, object> { [expressionNodeBuilder.GetNodeField(order.member)] = new { order = order.asc ? "asc" : "desc" } })
                                 .ToList();
            }

            // #3 skip take
            int skip = 0;
            if (combinedStream.skip > 0)
                queryBody["from"] = skip = combinedStream.skip.Value;

            var take = combinedStream.take >= 0 ? combinedStream.take.Value : maxResultWindowSize;
            if (take + skip > maxResultWindowSize) take = maxResultWindowSize - skip;
            queryBody["size"] = take;
            return queryBody;
        }





        public class QueryResponse<T>
        {
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
