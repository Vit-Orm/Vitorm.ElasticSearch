using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Vit.Linq;
using Vit.Linq.ExpressionTree.ComponentModel;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext
    {

        #region #2.2 Retrieve : Query
        public override IQueryable<Entity> Query<Entity>()
        {
            var indexName = GetIndex<Entity>();
            return Query<Entity>(indexName);
        }





        #region StreamReader

        public static StreamReader defaultStreamReader =
            ((Func<StreamReader>)(() =>
            {
                StreamReader streamReader = new StreamReader();
                streamReader.methodCallConvertors.Add(Queryable_Extensions_BatchAsync.Convert);
                return streamReader;
            }))();

        public StreamReader streamReader = defaultStreamReader;
        #endregion

        public virtual IQueryable<Entity> Query<Entity>(string indexName)
        {
            return QueryableBuilder.Build<Entity>(QueryExecutor, dbGroupName);

            #region QueryExecutor
            object QueryExecutor(Expression expression, Type expressionResultType)
            {
                // #1 convert to ExpressionNode
                ExpressionNode_Lambda node = convertService.ConvertToData_LambdaNode(expression, autoReduce: true, isArgument: QueryIsFromSameDb);
                //var strNode = Json.Serialize(node);

                // #2 convert to Stream
                var stream = streamReader.ReadFromNode(node);
                //var strStream = Json.Serialize(stream);

                // #3.3 Query
                // #3.3.1
                if (stream is StreamToUpdate) throw new NotSupportedException($"not supported {nameof(Orm_Extensions.ExecuteUpdate)}");
                var combinedStream = stream as CombinedStream;
                if (stream is SourceStream) combinedStream = new CombinedStream("tmp") { source = stream };

                if (combinedStream.method == nameof(Orm_Extensions.ExecuteDelete)) throw new NotSupportedException($"not supported {nameof(Orm_Extensions.ExecuteDelete)}");
                SourceStream source = combinedStream.source as SourceStream ?? throw new NotSupportedException("not supported nested query");
                if (combinedStream.isGroupedStream) throw new NotSupportedException("not supported group query");
                if (combinedStream.joins?.Any() == true) throw new NotSupportedException("not supported join query");
                if (combinedStream.distinct != null) throw new NotSupportedException("not supported distinct query");


                var queryPayload = ConvertStreamToQueryPayload(combinedStream);


                if (combinedStream.method == nameof(Queryable_Extensions.TotalCount) || combinedStream.method == nameof(Queryable.Count))
                {
                    var queryArg = (combinedStream.orders, combinedStream.skip, combinedStream.take);
                    (combinedStream.orders, combinedStream.skip, combinedStream.take) = (null, null, 0);

                    var count = Query<Entity>(queryPayload, indexName)?.hits?.total?.value ?? 0;

                    if (count > 0 && combinedStream.method == nameof(Queryable.Count))
                    {
                        if (queryArg.skip > 0) count = Math.Max(count - queryArg.skip.Value, 0);

                        if (queryArg.take.HasValue)
                            count = Math.Min(count, queryArg.take.Value);
                    }

                    return count;
                }


                if (combinedStream.method == nameof(Orm_Extensions.ToExecuteString))
                {
                    return Serialize(queryPayload);
                }

                Delegate select = null;
                if (combinedStream.select?.fields != null)
                {
                    //if (combinedStream.select.isDefaultSelect != true)
                    select = BuildSelect(source.GetEntityType(), combinedStream.select.fields, source.alias);
                }

                if (combinedStream.method == nameof(Queryable_Extensions.ToListAsync))
                {
                    var resultEntityType = expression.Type.GetGenericArguments()[0].GetGenericArguments()[0];
                    return MethodInfo_ToListAsync(typeof(Entity), resultEntityType).Invoke(null, new object[] { this, expression, queryPayload, select, indexName });
                }

                if (combinedStream.method == nameof(Queryable_Extensions_BatchAsync.BatchAsync))
                {
                    var resultEntityType = expression.Type.GetGenericArguments()[0].GetGenericArguments()[0];
                    var arg = new BatchAsyncArgument { dbContext = this, expression = expression, stream = combinedStream, queryPayload = queryPayload, indexName = indexName };
                    return MethodInfo_BatchAsync(typeof(Entity), resultEntityType).Invoke(null, new object[] { arg, select });
                }

                var searchResult = Query<Entity>(queryPayload, indexName);


                var entityDescriptor = GetEntityDescriptor(typeof(Entity));
                var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(this, entityDescriptor));



                // #3.3.2 execute and read result
                switch (combinedStream.method)
                {
                    case nameof(Queryable.FirstOrDefault):
                        {
                            var entity = entities.FirstOrDefault();
                            return select == null ? entity : select.DynamicInvoke(entity);
                        }
                    case nameof(Queryable.First):
                        {
                            var entity = entities.First();
                            return select == null ? entity : select.DynamicInvoke(entity);
                        }
                    case nameof(Queryable.LastOrDefault):
                        {
                            var entity = entities.LastOrDefault();
                            return select == null ? entity : select.DynamicInvoke(entity);
                        }
                    case nameof(Queryable.Last):
                        {
                            var entity = entities.Last();
                            return select == null ? entity : select.DynamicInvoke(entity);
                        }
                    case nameof(Queryable_Extensions.ToListAndTotalCount):
                        {
                            IList list; int totalCount;

                            // #1 ToList
                            {
                                if (select == null)
                                {
                                    list = entities.ToList();
                                }
                                else
                                {
                                    // ToList
                                    var resultEntityType = expression.Type.GetGenericArguments()?.FirstOrDefault()?.GetGenericArguments()?.FirstOrDefault();
                                    list = Activator.CreateInstance(typeof(List<>).MakeGenericType(resultEntityType)) as IList;
                                    foreach (var entity in entities)
                                    {
                                        list.Add(select.DynamicInvoke(entity));
                                    }
                                }
                            }

                            // #2 TotalCount
                            totalCount = searchResult?.hits?.total?.value ?? 0;

                            return new Func<object, int, (object, int)>(ValueTuple.Create<object, int>)
                                .Method.GetGenericMethodDefinition()
                                .MakeGenericMethod(list.GetType(), typeof(int))
                                .Invoke(null, new object[] { list, totalCount });

                        }
                    case nameof(Enumerable.ToList):
                    case "":
                    case null:
                        {
                            IList list;

                            if (select == null)
                            {
                                list = entities.ToList();
                            }
                            else
                            {
                                // ToList
                                var resultEntityType = expression.Type.GetGenericArguments()[0];
                                list = Activator.CreateInstance(typeof(List<>).MakeGenericType(resultEntityType)) as IList;
                                foreach (var entity in entities)
                                {
                                    list.Add(select.DynamicInvoke(entity));
                                }
                            }
                            return list;
                        }
                }
                throw new NotSupportedException("not supported query type: " + combinedStream.method);
            }

            #endregion
        }

        /// <summary>
        /// to identify whether contexts are from the same database
        /// </summary>
        protected string dbGroupName { get; set; }
        protected virtual bool QueryIsFromSameDb(object query, Type elementType)
        {
            return dbGroupName == QueryableBuilder.GetQueryConfig(query as IQueryable) as string;
        }

        #endregion


    }
}
