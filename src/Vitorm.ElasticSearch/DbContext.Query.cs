using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Vit.Linq;
using Vit.Linq.ExpressionNodes;
using Vit.Linq.ExpressionNodes.ComponentModel;

using Vitorm.ElasticSearch.QueryExecutor;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext : Vitorm.DbContext
    {
        public static Func<Entity, Result> BuildSelect<Entity, Result>(CombinedStream combinedStream, ExpressionConvertService convertService)
        {
            ExpressionNode selectedFields = combinedStream.select?.fields;
            string entityParameterName = combinedStream.source?.alias;

            if (selectedFields == null) return null;

            // Compile Lambda
            var lambdaNode = ExpressionNode.Lambda(new[] { entityParameterName }, selectedFields);
            //var strNode = Json.Serialize(lambdaNode);

            var entityType = typeof(Entity);
            var lambdaExp = convertService.ConvertToCode_LambdaExpression(lambdaNode, new[] { entityType });
            return (Func<Entity, Result>)lambdaExp.Compile();
        }


        #region QueryExecutor
        public static Dictionary<string, IQueryExecutor> defaultQueryExecutors = CreateDefaultQueryExecutors();
        public static Dictionary<string, IQueryExecutor> CreateDefaultQueryExecutors()
        {
            Dictionary<string, IQueryExecutor> defaultQueryExecutors = new();

            #region AddDefaultQueryExecutor
            void AddDefaultQueryExecutor(IQueryExecutor queryExecutor, string methodName = null)
            {
                defaultQueryExecutors[methodName ?? queryExecutor.methodName] = queryExecutor;
            }
            #endregion



            #region Sync
            // Orm_Extensions
            //AddDefaultQueryExecutor(ExecuteUpdate.Instance);
            //AddDefaultQueryExecutor(ExecuteDelete.Instance);
            AddDefaultQueryExecutor(ToExecuteString.Instance);

            // ToList
            AddDefaultQueryExecutor(Vitorm.ElasticSearch.QueryExecutor.ToList.Instance);
            // Count TotalCount
            AddDefaultQueryExecutor(Count.Instance);
            AddDefaultQueryExecutor(Count.Instance, methodName: nameof(Queryable_Extensions.TotalCount));

            // ToListAndTotalCount
            AddDefaultQueryExecutor(ToListAndTotalCount.Instance);

            // FirstOrDefault First LastOrDefault Last
            AddDefaultQueryExecutor(FirstOrDefault.Instance);
            AddDefaultQueryExecutor(FirstOrDefault.Instance, methodName: nameof(Queryable.First));
            AddDefaultQueryExecutor(FirstOrDefault.Instance, methodName: nameof(Queryable.LastOrDefault));
            AddDefaultQueryExecutor(FirstOrDefault.Instance, methodName: nameof(Queryable.Last));
            #endregion


            #region Async
            // Orm_Extensions
            //AddDefaultQueryExecutor(ExecuteUpdateAsync.Instance);
            //AddDefaultQueryExecutor(ExecuteDeleteAsync.Instance);

            // ToList
            AddDefaultQueryExecutor(Vitorm.ElasticSearch.QueryExecutor.ToListAsync.Instance);
            // Count TotalCount
            AddDefaultQueryExecutor(CountAsync.Instance);
            AddDefaultQueryExecutor(CountAsync.Instance, methodName: nameof(Queryable_AsyncExtensions.TotalCountAsync));

            // ToListAndTotalCount
            AddDefaultQueryExecutor(ToListAndTotalCountAsync.Instance);

            // FirstOrDefault First LastOrDefault Last
            AddDefaultQueryExecutor(FirstOrDefaultAsync.Instance);
            AddDefaultQueryExecutor(FirstOrDefaultAsync.Instance, methodName: nameof(Queryable_AsyncExtensions.FirstAsync));
            AddDefaultQueryExecutor(FirstOrDefaultAsync.Instance, methodName: nameof(Queryable_AsyncExtensions.LastOrDefaultAsync));
            AddDefaultQueryExecutor(FirstOrDefaultAsync.Instance, methodName: nameof(Queryable_AsyncExtensions.LastAsync));


            AddDefaultQueryExecutor(BatchAsync.Instance);
            #endregion


            return defaultQueryExecutors;
        }
        public Dictionary<string, IQueryExecutor> queryExecutors = defaultQueryExecutors;
        #endregion

        #region #2.2 Retrieve : Query
        public override IQueryable<Entity> Query<Entity>()
        {
            var indexName = GetIndex<Entity>();
            return Query<Entity>(indexName);
        }

        #region StreamReader
        public static StreamReader defaultStreamReader = new StreamReader();
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

                // #3
                if (stream is not CombinedStream combinedStream) combinedStream = new CombinedStream("tmp") { source = stream };

                // #4 validate
                if (stream is StreamToUpdate) throw new NotSupportedException($"not supported {nameof(Orm_Extensions.ExecuteUpdate)}");
                SourceStream source = combinedStream.source as SourceStream ?? throw new NotSupportedException("not supported nested query");
                if (combinedStream.isGroupedStream) throw new NotSupportedException("not supported group query");
                if (combinedStream.joins?.Any() == true) throw new NotSupportedException("not supported join query");
                if (combinedStream.distinct != null) throw new NotSupportedException("not supported distinct query");


                #region #5 Execute by registered executor
                {
                    var executorArg = new QueryExecutorArgument
                    {
                        combinedStream = combinedStream,
                        dbContext = this,
                        indexName = indexName,
                        expression = expression
                    };

                    var method = combinedStream.method;
                    if (string.IsNullOrWhiteSpace(method)) method = nameof(Enumerable.ToList);
                    if (queryExecutors.TryGetValue(method, out var queryExecutor))
                    {
                        return queryExecutor.ExecuteQuery(executorArg);
                    }
                }
                #endregion

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
