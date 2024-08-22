using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class CountAsync : IQueryExecutor
    {
        public static readonly CountAsync Instance = new();

        public string methodName => nameof(Queryable_AsyncExtensions.CountAsync);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var entityType = (execArg.combinedStream.source as SourceStream).GetEntityType();
            return Execute_MethodInfo(entityType).Invoke(null, new object[] { execArg });
        }


        public static async Task<int> Execute<Entity>(QueryExecutorArgument execArg)
        {
            CombinedStream combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;

            // #1 queryPayload
            var queryArg = (combinedStream.orders, combinedStream.skip, combinedStream.take, combinedStream.method);
            (combinedStream.orders, combinedStream.skip, combinedStream.take, combinedStream.method) = (null, null, 0, nameof(Queryable.Count));
            var queryPayload = dbContext.ConvertStreamToQueryPayload(combinedStream);

            // #2 query in server
            var count = (await dbContext.QueryAsync<Entity>(queryPayload, execArg.indexName))?.hits?.total?.value ?? 0;

            // #3 result
            if (count > 0 && queryArg.method.StartsWith(nameof(Queryable.Count)))
            {
                if (queryArg.skip > 0) count = Math.Max(count - queryArg.skip.Value, 0);

                if (queryArg.take.HasValue)
                    count = Math.Min(count, queryArg.take.Value);
            }

            (combinedStream.orders, combinedStream.skip, combinedStream.take, combinedStream.method) = queryArg;
            return count;
        }


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, Task<int>>(Execute<object>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType);

    }
}
