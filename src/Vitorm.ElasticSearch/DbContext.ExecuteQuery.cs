using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
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

            var searchContent = new StringContent(query, Encoding.UTF8, "application/json");
            var httpResponse = await httpClient.PostAsync(searchUrl, searchContent);

            var strResponse = await httpResponse.Content.ReadAsStringAsync();
            if (!httpResponse.IsSuccessStatusCode) throw new Exception(strResponse);

            return strResponse;
        }


        public static ElasticSearchQueryBuilder defaultQueryBuilder;

        public ElasticSearchQueryBuilder queryBuilder = (defaultQueryBuilder ?? (defaultQueryBuilder = new()));

        public virtual object BuildElasticSearchQuery(CombinedStream combinedStream)
        {
            var queryBody = new Dictionary<string, object>();

            // #1 where
            queryBody["query"] = queryBuilder.ConvertToQuery(combinedStream.where);

            // #2 orders
            if (combinedStream.orders?.Any() == true)
            {
                queryBody["sort"] = combinedStream.orders
                                 .Select(order => new Dictionary<string, object> { [queryBuilder.GetNodeField(order.member)] = new { order = order.asc ? "asc" : "desc" } })
                                 .ToList();
            }

            // #3 skip take
            if (combinedStream.skip.HasValue)
                queryBody["from"] = combinedStream.skip.Value;
            if (combinedStream.take.HasValue)
                queryBody["size"] = combinedStream.take.Value;
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

                    public T GetSource(IEntityDescriptor entityDescriptor)
                    {
                        if (_source != null && _id != null)
                            entityDescriptor?.key?.SetValue(_source, _id);
                        return _source;
                    }
                }
            }
        }



    }
}
