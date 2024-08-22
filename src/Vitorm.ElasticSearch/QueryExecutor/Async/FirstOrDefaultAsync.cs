using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class FirstOrDefaultAsync : IQueryExecutor
    {
        public static readonly FirstOrDefaultAsync Instance = new();

        public string methodName => nameof(Queryable_AsyncExtensions.FirstOrDefaultAsync);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var entityType = (execArg.combinedStream.source as SourceStream)?.GetEntityType();
            var resultType = execArg.expression.Type.GetGenericArguments().First();
            var resultEntityType = resultType;

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { execArg });
        }


        public static async Task<Result> Execute<Entity, Result>(QueryExecutorArgument execArg)
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
            Entity entity;
            var method = combinedStream.method;
            if (method.EndsWith("Async")) method = method.Substring(0, method.Length - "Async".Length);
            switch (method)
            {
                case nameof(Queryable.FirstOrDefault):
                    {
                        entity = entities.FirstOrDefault(); break;
                    }
                case nameof(Queryable.First):
                    {
                        entity = entities.First(); break;
                    }
                case nameof(Queryable.LastOrDefault):
                    {
                        entity = entities.LastOrDefault(); break;
                    }
                case nameof(Queryable.Last):
                    {
                        entity = entities.Last(); break;
                    }
                default: throw new NotSupportedException("not supported query type: " + combinedStream.method);

            }
            return funcSelect == null ? ((Result)(object)entity) : funcSelect(entity);
        }


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, Task<string>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

    }
}
