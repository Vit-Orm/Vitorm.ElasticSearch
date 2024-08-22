using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class BatchAsync : IQueryExecutor
    {
        public static readonly BatchAsync Instance = new();

        public string methodName => nameof(Queryable_Extensions_BatchAsync.BatchAsync);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var entityType = (execArg.combinedStream.source as SourceStream)?.GetEntityType();
            var resultType = execArg.expression.Type.GetGenericArguments().First();
            var resultEntityType = resultType.GetGenericArguments().First();

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { execArg });
        }


        public static IAsyncEnumerable<List<Result>> Execute<Entity, Result>(QueryExecutorArgument execArg)
            where Entity : class
        {
            CombinedStream combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;

            // #1 queryPayload
            var queryPayload = dbContext.ConvertStreamToQueryPayload(combinedStream);

            int maxResultCount = int.MaxValue;
            if (combinedStream.take >= 0) maxResultCount = combinedStream.take.Value;

            int batch = 5000;
            if (combinedStream.methodArguments[0] is int _batch) batch = _batch;
            queryPayload["size"] = batch;

            int scrollCacheMinutes = 1;
            bool useDefaultSort = false;

            if (combinedStream.methodArguments[1] is int _scrollCacheMinutes) scrollCacheMinutes = _scrollCacheMinutes;
            if (combinedStream.methodArguments[2] is bool _useDefaultSort) useDefaultSort = _useDefaultSort;

            var queryArg = new DbContext.ScrollQueryArgument
            {
                queryPayload = queryPayload,
                indexName = execArg.indexName,
                scrollCacheMinutes = scrollCacheMinutes,
                useDefaultSort = useDefaultSort,
                maxResultCount = maxResultCount
            };

            var asyncEnumerable = dbContext.BatchQueryAsync<Entity>(queryArg);


            // #3 funcSelect
            Func<Entity, Result> funcSelect = DbContext.BuildSelect<Entity, Result>(combinedStream, dbContext.convertService);

            // #4 result
            if (funcSelect == null || typeof(Entity) == typeof(Result)) return (IAsyncEnumerable<List<Result>>)asyncEnumerable;
            return asyncEnumerable.Select(funcSelect);
        }


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, IAsyncEnumerable<List<string>>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

    }
}
