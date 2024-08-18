using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Vit.Linq;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class ToListAndTotalCount : IQueryExecutor
    {
        public static readonly ToListAndTotalCount Instance = new();

        public string methodName => nameof(Queryable_Extensions.ToListAndTotalCount);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var entityType = (execArg.combinedStream.source as SourceStream).GetEntityType();
            var resultType = execArg.expression.Type;
            var resultEntityType = resultType.GetGenericArguments().First().GetGenericArguments().First();

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { execArg });
        }


        public static (List<Result> list, int totalCount) Execute<Entity, Result>(QueryExecutorArgument execArg)
            => ToListAndTotalCountAsync.Execute<Entity, Result>(execArg).Result;


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, (List<string> list, int totalCount)>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

    }
}
