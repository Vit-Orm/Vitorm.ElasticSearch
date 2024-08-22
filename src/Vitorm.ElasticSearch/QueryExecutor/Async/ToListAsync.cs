using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Vit.Linq;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class ToListAsync : IQueryExecutor
    {
        public static readonly ToListAsync Instance = new();

        public string methodName => nameof(Queryable_Extensions.ToListAsync);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var entityType = (execArg.combinedStream.source as SourceStream)?.GetEntityType();
            var resultType = execArg.expression.Type.GetGenericArguments().First();
            var resultEntityType = resultType.GetGenericArguments().First();

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { execArg });
        }


        public static async Task<List<Result>> Execute<Entity, Result>(QueryExecutorArgument execArg)
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
            if (funcSelect == null)
            {
                return entities.ToList() as List<Result>;
            }
            else
            {
                return entities.Select(entity => funcSelect(entity)).ToList();
            }
        }


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, Task<List<string>>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

    }
}
