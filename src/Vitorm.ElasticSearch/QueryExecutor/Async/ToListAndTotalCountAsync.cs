using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class ToListAndTotalCountAsync : IQueryExecutor
    {
        public static readonly ToListAndTotalCountAsync Instance = new();

        public string methodName => nameof(Queryable_AsyncExtensions.ToListAndTotalCountAsync);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var entityType = (execArg.combinedStream.source as SourceStream).GetEntityType();
            var resultType = execArg.expression.Type.GetGenericArguments().First();
            var resultEntityType = resultType.GetGenericArguments().First().GetGenericArguments().First();

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { execArg });
        }


        public static async Task<(List<Result> list, int totalCount)> Execute<Entity, Result>(QueryExecutorArgument execArg)
        {
            var combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;

            var searchArg = new SearchExecutorArgument<Result> { combinedStream = execArg.combinedStream, dbContext = dbContext, indexName = execArg.indexName };
            searchArg.needList = true;
            searchArg.needTotalCount = true;


            await dbContext.ExecuteSearchAsync<Entity, Result>(searchArg);

            List<Result> list = searchArg.list.ToList();
            int totalCount = searchArg.totalCount ?? 0;

            return (list, totalCount);
        }


        private static MethodInfo Execute_MethodInfo_;
        public static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, Task<(List<string> list, int totalCount)>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

    }
}
