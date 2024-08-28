using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Vit.Linq.ComponentModel;
using Vit.Linq.ExpressionNodes.ComponentModel;

using Vitorm.ElasticSearch.QueryBuilder;
using Vitorm.ElasticSearch.QueryExecutor;
using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.SearchExecutor
{
    public class GroupExecutor : ISearchExecutor
    {
        public async Task<bool> ExecuteSearchAsync<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            var combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;

            if (combinedStream.source is not SourceStream) return false;
            if (!combinedStream.isGroupedStream) return false;
            if (combinedStream.joins?.Any() == true) return false;
            if (combinedStream.distinct != null) return false;

            Type keyType;
            var resultSelector = combinedStream.select.resultSelector;
            if (resultSelector == null)
            {
                keyType = typeof(ResultEntity);
            }
            else
            {
                var groupType = resultSelector.Lambda_GetParamTypes()[0];
                keyType = groupType.GetGenericArguments()[0];
            }

            var task = Execute_MethodInfo(typeof(Entity), typeof(ResultEntity), keyType).Invoke(null, new[] { arg }) as Task;
            await task.ConfigureAwait(false);

            return true;
        }

        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType, Type keyType) =>
            (Execute_MethodInfo_ ??= new Func<SearchExecutorArgument<string>, Task>(ExecuteAsync<string, string, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType, keyType);

        static async Task ExecuteAsync<Entity, ResultEntity, Key>(SearchExecutorArgument<ResultEntity> arg)
        {
            var combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;

            var queryPayload = ConvertGroupQueryPayload<Entity, ResultEntity>(arg);


            if (combinedStream.groupByFields?.nodeType == NodeType.Member)
            {
                var searchResult = await dbContext.ExecuteSearchAsync<AggregationResult<Entity, WrapKey<Key>>>(queryPayload, indexName: arg.indexName);

                // convert list
                if (arg.needList)
                {
                    var lambdaExpression = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();
                    var delSelect = (Func<IGrouping<Key, Entity>, ResultEntity>)lambdaExpression.Compile();

                    var groups = searchResult?.aggregations?.groupedDoc?.buckets?.Select(
                        bucket => new Grouping<Key, Entity>(
                            bucket.key.Key,
                            bucket.doc?.hits?.hits?.Select(m => m._source
                        )));
                    arg.list = groups.Select(delSelect);
                }
            }
            else
            {
                var searchResult = await dbContext.ExecuteSearchAsync<AggregationResult<Entity, Key>>(queryPayload, indexName: arg.indexName);

                // convert list
                if (arg.needList)
                {
                    var lambdaExpression = combinedStream.select.resultSelector.Lambda_GetLambdaExpression();
                    var delSelect = (Func<IGrouping<Key, Entity>, ResultEntity>)lambdaExpression.Compile();

                    var groups = searchResult?.aggregations?.groupedDoc?.buckets?.Select(
                        bucket => new Grouping<Key, Entity>(
                            bucket.key,
                            bucket.doc?.hits?.hits?.Select(m => m._source
                        )));
                    arg.list = groups.Select(delSelect);
                }
            }
        }

        class WrapKey<TKey>
        {
            public TKey Key;
        }


        class Grouping<TKey, TElement> : IGrouping<TKey, TElement>
        {
            public Grouping(TKey Key, IEnumerable<TElement> list) { this.Key = Key; this.list = list; }
            public TKey Key { get; private set; }

            IEnumerable<TElement> list;

            public IEnumerator<TElement> GetEnumerator() => list.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }


        static Dictionary<string, object> ConvertGroupQueryPayload<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            var combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;

            var queryPayload = new Dictionary<string, object>();

            // #1 where
            queryPayload["query"] = dbContext.expressionNodeBuilder.ConvertToQuery(combinedStream.where);
            queryPayload["size"] = 0;

            // #2 Aggs
            {
                var aggs = new Dictionary<string, object>();
                aggs["groupedDoc"] = BuildAggr_GroupBy<Entity, ResultEntity>(arg);

                queryPayload["aggs"] = aggs;
            }

            return queryPayload;
        }



        static object BuildAggr_GroupBy<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            var combinedStream = arg.combinedStream;

            var groupFields = GetGroupFields<Entity, ResultEntity>(arg);
            var orderFields = GetOrderFields<Entity, ResultEntity>(arg);

            var groupByFields = groupFields.Select((groupField, index) =>
                   {
                       var orderField = orderFields.FirstOrDefault(order => order.field == groupField.field);
                       if (orderField.field == null) orderField = (null, true, index + orderFields.Count);

                       return (groupField.field, orderField.index, groupField: (groupField.field, groupField.fieldAs, orderField.asc, groupField.fieldType));
                   })
                   .OrderBy(m => m.index)
                   .GroupBy(m => m.field).Select(g => g.First().groupField) // remove duplicate fields
                   .ToList();

            var range = new RangeInfo(skip: combinedStream.skip ?? 0, take: combinedStream.take ?? 0);
            return BuildAggr_GroupBy_Composite(groupByFields, range);
        }


        static List<(string field, string fieldAs, Type fieldType)> GetGroupFields<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            var combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var expressionNodeBuilder = dbContext.expressionNodeBuilder;

            var node = combinedStream.groupByFields;
            List<(string field, string fieldAs, Type fieldType)> fields = new();
            var nodeConvertArg = new ExpressionNodeConvertArgument { builder = expressionNodeBuilder };
            if (node?.nodeType == NodeType.New)
            {
                ExpressionNode_New newNode = node;
                newNode.constructorArgs.ForEach(nodeArg =>
                {
                    var fieldAs = nodeArg.name;
                    var field = expressionNodeBuilder.GetNodeField(nodeConvertArg, nodeArg.value, out var fieldType);
                    fields.Add((field, fieldAs, fieldType));
                });
            }
            else if (node?.nodeType == NodeType.Member)
            {
                var field = expressionNodeBuilder.GetNodeField(nodeConvertArg, node, out var fieldType);
                var fieldAs = "Key";
                fields.Add((field, fieldAs, fieldType));
            }
            else
            {
                throw new NotSupportedException("[GroupExecutor] groupByFields is not valid: NodeType must be New or Member");
            }
            return fields;
        }

        static List<(string field, bool asc, int index)> GetOrderFields<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            var combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;
            var expressionNodeBuilder = dbContext.expressionNodeBuilder;

            var nodeConvertArg = new ExpressionNodeConvertArgument { builder = expressionNodeBuilder };
            return combinedStream.orders?.Select((orderField, index) =>
            {
                var field = expressionNodeBuilder.GetNodeField(nodeConvertArg, orderField.member, out var fieldType);
                return (field, orderField.asc, index);
            }).ToList() ?? new();
        }



        static object BuildAggr_GroupBy_Composite(List<(string field, string fieldAs, bool asc, Type fieldType)> groupByFields, RangeInfo range, object after_key = null)
        {
            Dictionary<string, object> composite = new();
            Dictionary<string, object> aggs = new();

            // #1 sources
            var sources = groupByFields.Select(groupByField =>
            {
                var field = groupByField.field;
                var fieldType = groupByField.fieldType;
                if (fieldType == typeof(string) && !field.EndsWith(".keyword"))
                    field += ".keyword";
                return new Dictionary<string, object> { [groupByField.fieldAs] = new { terms = new { field, missing_bucket = true, order = groupByField.asc ? "asc" : "desc" } } };

            });
            composite["sources"] = sources;
            if (after_key != null) composite["after"] = after_key;

            // #2 range
            var size = range.skip + range.take;
            if (size > 0)
            {
                composite["size"] = size;
                if (range.skip > 0)
                    aggs["r_bucket_sort"] = new { bucket_sort = new { from = range.skip, size = range.take } };
            }


            // #3 get First item in bucket
            // new { top_hits = new { size = 1 } };
            aggs["doc"] = new { top_hits = new { } };

            return new Dictionary<string, object> { ["composite"] = composite, ["aggs"] = aggs };

        }


        /*
https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-composite-aggregation.html

[get] /user/_search
// request:
{
    "query": {
        "match_all": {}
    },
    "size": 0,
    "aggs": {
        "groupedDoc": {
            "composite": {
                "sources": [
                    {
                        "fatherId": {
                            "terms": {
                                "field": "fatherId",
                                "missing_bucket": true,
                                "order": "desc"
                            }
                        }
                    },
                    {
                        "motherId": {
                            "terms": {
                                "field": "motherId",
                                "missing_bucket": true,
                                "order": "desc"
                            }
                        }
                    }
                ]
            },
            "aggs": {
                "doc": {
                    "top_hits": {}
                }
            }
        }
    }
}

// response:
{
  "aggregations": {
    "groupedDoc": {
      "after_key": {
        "fatherId": null,
        "motherId": null
      },
      "buckets": [
        {
          "key": {
            "fatherId": 5,
            "motherId": 6
          },
          "doc_count": 1,
          "doc": {
            "hits": {
              "total": {
                "value": 1,
                "relation": "eq"
              },
              "max_score": 1,
              "hits": [
                {
                  "_index": "user",
                  "_type": "_doc",
                  "_id": "3",
                  "_score": 1,
                  "_source": {
                    "id": 3,
                    "name": "u356",
                    "fatherId": 5,
                    "motherId": 6
                  }
                }
              ]
            }
          }
        }
      ]
    }
  }
}
         
         
         
         */
        class AggregationResult<Entity, KeyType>
        {
            public Aggregation aggregations;
            public class Aggregation
            {
                public ToListAndTotalCount totalCount;
                public GroupedDoc groupedDoc;
                public class TotalCount { public int value; }
                public class GroupedDoc
                {
                    public object after_key;
                    public List<Bucket> buckets;
                }
                public class Bucket
                {
                    public KeyType key;
                    public Doc doc;
                    public class Doc
                    {
                        public HitsContainer hits;
                        public class HitsContainer
                        {
                            public List<Hit> hits;
                            public class Hit
                            {
                                public string _index;
                                public string _type;
                                public string _id;
                                public float? _score;
                                public Entity _source;
                            }
                        }

                    }
                }
            }
        }

    }
}
