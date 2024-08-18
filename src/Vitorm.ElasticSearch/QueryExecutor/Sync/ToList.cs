using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class ToList : IQueryExecutor
    {
        public static readonly ToList Instance = new();

        public string methodName => nameof(Enumerable.ToList);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var entityType = (execArg.combinedStream.source as SourceStream).GetEntityType();
            var resultType = execArg.expression.Type;
            var resultEntityType = resultType.GetGenericArguments().First();

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { execArg });
        }


        public static List<Result> Execute<Entity, Result>(QueryExecutorArgument execArg)
           => ToListAsync.Execute<Entity, Result>(execArg).Result;


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, List<string>>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

    }
}
