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
            CombinedStream combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;

            // #1 queryPayload
            var queryPayload = dbContext.ConvertStreamToQueryPayload(combinedStream);

            // #2 query in server
            var searchResult = await dbContext.QueryAsync<Entity>(queryPayload, execArg.indexName);
            var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));
            var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(dbContext, entityDescriptor));


            // #3 funcSelect
            Func<Entity, Result> funcSelect = DbContext.BuildSelect<Entity, Result>(combinedStream, dbContext.convertService);

            // #4 result
            List<Result> list; int totalCount;
            if (funcSelect == null)
            {
                list = entities.ToList() as List<Result>;
            }
            else
            {
                list = entities.Select(entity => funcSelect(entity)).ToList();
            }
            // TotalCount
            totalCount = searchResult?.hits?.total?.value ?? 0;

            return (list, totalCount);
        }


        private static MethodInfo Execute_MethodInfo_;
        public static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, Task<(List<string> list, int totalCount)>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

    }
}
