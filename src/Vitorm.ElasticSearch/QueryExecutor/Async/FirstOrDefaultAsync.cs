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
            var combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;

            var searchArg = new SearchExecutorArgument<Result> { combinedStream = execArg.combinedStream, dbContext = execArg.dbContext, indexName = execArg.indexName };
            searchArg.needList = true;
            searchArg.needTotalCount = false;

            await dbContext.ExecuteSearchAsync<Entity, Result>(searchArg);

            // result 
            var method = combinedStream.method;
            if (method.EndsWith("Async")) method = method.Substring(0, method.Length - "Async".Length);
            switch (method)
            {
                case nameof(Queryable.FirstOrDefault):
                    {
                        return searchArg.list.FirstOrDefault();
                    }
                case nameof(Queryable.First):
                    {
                        return searchArg.list.First();
                    }
                case nameof(Queryable.LastOrDefault):
                    {
                        return searchArg.list.LastOrDefault();
                    }
                case nameof(Queryable.Last):
                    {
                        return searchArg.list.Last();
                    }
                default: throw new NotSupportedException("not supported query type: " + combinedStream.method);
            }
        }


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, Task<string>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

    }
}
