using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext
    {
        public class BatchAsyncArgument
        {
            public DbContext dbContext;
            public Expression expression;
            public CombinedStream stream;
            public Dictionary<string, object> queryPayload;
            public string indexName;
        }

        protected static IAsyncEnumerable<List<Result>> BatchAsync<Entity, Result>(BatchAsyncArgument arg, Func<Entity, Result> select)
            where Entity : class
        {
            var queryPayload = arg.queryPayload;

            int maxResultCount = int.MaxValue;
            if (arg.stream.take >= 0) maxResultCount = arg.stream.take.Value;

            int batch = 5000;
            if (arg.stream?.methodArguments[0] is int _batch) batch = _batch;
            queryPayload["size"] = batch;

            int scrollCacheMinutes = 1;
            bool useDefaultSort = false;

            if (arg.stream?.methodArguments[1] is int _scrollCacheMinutes) scrollCacheMinutes = _scrollCacheMinutes;
            if (arg.stream?.methodArguments[2] is bool _useDefaultSort) useDefaultSort = _useDefaultSort;

            var queryArg = new ScrollQueryArgument
            {
                queryPayload = queryPayload,
                indexName = arg.indexName,
                scrollCacheMinutes = scrollCacheMinutes,
                useDefaultSort = useDefaultSort,
                maxResultCount = maxResultCount
            };

            var asyncEnumerable = arg.dbContext.BatchQueryAsync<Entity>(queryArg);

            if (select == null || typeof(Entity) == typeof(Result)) return (IAsyncEnumerable<List<Result>>)asyncEnumerable;

            return asyncEnumerable.Select(select);
        }



        #region Method cache
        protected static MethodInfo MethodInfo_BatchAsync(Type entityType, Type resultEntityType) =>
            (MethodInfo_BatchAsync_ ??=
                 new Func<BatchAsyncArgument, Func<object, string>, IAsyncEnumerable<List<string>>>(BatchAsync)
                .GetMethodInfo().GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);


        private static MethodInfo MethodInfo_BatchAsync_;
        #endregion

    }
}
