using System;
using System.Linq;
using System.Reflection;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class Count : IQueryExecutor
    {
        public static readonly Count Instance = new();

        public string methodName => nameof(Enumerable.Count);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var entityType = (execArg.combinedStream.source as SourceStream).GetEntityType();
            return Execute_MethodInfo(entityType).Invoke(null, new object[] { execArg });
        }


        public static int Execute<Entity>(QueryExecutorArgument execArg)
                => CountAsync.Execute<Entity>(execArg).Result;


        private static MethodInfo Execute_MethodInfo_;
        static MethodInfo Execute_MethodInfo(Type entityType) =>
            (Execute_MethodInfo_ ??= new Func<QueryExecutorArgument, int>(Execute<object>).Method.GetGenericMethodDefinition())
            .MakeGenericMethod(entityType);

    }
}
