using System;
using System.Linq;
using System.Reflection;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class FirstOrDefault : IQueryExecutor
    {
        public static readonly FirstOrDefault Instance = new();

        public string methodName => nameof(Enumerable.FirstOrDefault);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var entityType = (execArg.combinedStream.source as SourceStream)?.GetEntityType();
            var resultType = execArg.expression.Type;
            var resultEntityType = resultType;

            return Execute_MethodInfo(entityType, resultEntityType).Invoke(null, new object[] { execArg });
        }


        public static Result Execute<Entity, Result>(QueryExecutorArgument execArg)
            => FirstOrDefaultAsync.Execute<Entity, Result>(execArg).Result;


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType, Type resultEntityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, string>(Execute<object, string>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);

    }
}
