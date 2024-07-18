using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

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

        protected static async Task<List<Result>> ToListAsync<Entity, Result>(DbContext self, Expression expression, object queryPayload, Func<Entity, Result> select, string indexName)
        {
            var searchResult = await self.QueryAsync<Entity>(queryPayload, indexName);

            var entityDescriptor = self.GetEntityDescriptor(typeof(Entity));
            var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(entityDescriptor));

            if (select == null)
            {
                return entities.ToList() as List<Result>;
            }
            else
            {
                return entities.Select(entity => select(entity)).ToList();
            }
        }


        #region Method cache
        private static MethodInfo MethodInfo_ToListAsync_;
        static MethodInfo MethodInfo_ToListAsync(Type entityType, Type resultEntityType) =>
            (MethodInfo_ToListAsync_ ??=
                 new Func<DbContext, Expression, object, Func<object, string>, string, Task<List<string>>>(ToListAsync)
                .GetMethodInfo().GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

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
                var stream = StreamReader.ReadNode(node);
                //var strStream = Json.Serialize(stream);

                // #3.3 Query
                // #3.3.1
                if (stream is not CombinedStream combinedStream) combinedStream = new CombinedStream("tmp") { source = stream };
                SourceStream source = combinedStream.source as SourceStream ?? throw new NotSupportedException("not supported nested query");
                if (combinedStream.isGroupedStream) throw new NotSupportedException("not supported group query");
                if (combinedStream.joins?.Any() == true) throw new NotSupportedException("not supported join query");
                if (combinedStream.distinct != null) throw new NotSupportedException("not supported distinct query");

                if (combinedStream.method == nameof(Queryable_Extensions.TotalCount) || combinedStream.method == nameof(Queryable.Count))
                {
                    var queryArg = (combinedStream.orders, combinedStream.skip, combinedStream.take);
                    (combinedStream.orders, combinedStream.skip, combinedStream.take) = (null, null, 0);

                    var count = Query<Entity>(BuildElasticSearchQuery(combinedStream), indexName)?.hits?.total?.value ?? 0;

                    if (count > 0 && combinedStream.method == nameof(Queryable.Count))
                    {
                        if (queryArg.skip > 0) count = Math.Max(count - queryArg.skip.Value, 0);

                        if (queryArg.take.HasValue)
                            count = Math.Min(count, queryArg.take.Value);
                    }

                    return count;
                }


                var queryPayload = BuildElasticSearchQuery(combinedStream);

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
                    return MethodInfo_ToListAsync(typeof(Entity), resultEntityType).Invoke(null, new[] { this, expression, queryPayload, select, indexName });
                }

                var searchResult = Query<Entity>(queryPayload, indexName);


                var entityDescriptor = GetEntityDescriptor(typeof(Entity));
                var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(entityDescriptor));



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
