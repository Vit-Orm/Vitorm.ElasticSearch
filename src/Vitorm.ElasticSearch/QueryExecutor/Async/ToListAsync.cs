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
            var combinedStream = execArg.combinedStream;
            var dbContext = execArg.dbContext;

            var searchArg = new SearchExecutorArgument<Result> { combinedStream = execArg.combinedStream, dbContext = execArg.dbContext, indexName = execArg.indexName };
            searchArg.getList = true;
            searchArg.getTotalCount = false;

            await dbContext.ExecuteSearchAsync<Entity, Result>(searchArg);

            return searchArg.list.ToList();
        }




        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, Task<List<string>>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

    }
}
