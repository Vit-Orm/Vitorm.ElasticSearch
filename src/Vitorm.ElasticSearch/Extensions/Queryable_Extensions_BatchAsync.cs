using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Vit.Linq.ExpressionNodes;
using Vit.Linq.ExpressionNodes.ComponentModel;

using Vitorm.StreamQuery;
using Vitorm.StreamQuery.MethodCall;

namespace Vitorm.ElasticSearch
{

    public static partial class Queryable_Extensions_BatchAsync
    {
        public static IEnumerable<Result> ToEnumerableByBatch<Result>(this IQueryable<Result> source, int batchSize = 5000, int scrollCacheMinutes = 1, bool useDefaultSort = false)
        {
            return source?.BatchAsync(batchSize, scrollCacheMinutes, useDefaultSort).ToEnumerable().FlattenEnumerable();
        }


        [ExpressionNode_CustomMethod]
        [StreamQueryCustomMethod_BatchAsync]
        public static IAsyncEnumerable<List<Result>> BatchAsync<Result>(this IQueryable<Result> source, int batchSize = 5000, int scrollCacheMinutes = 1, bool useDefaultSort = false)
        {
            if (source == null)
                throw new ArgumentNullException(nameof(source));

            return source.Provider.Execute<IAsyncEnumerable<List<Result>>>(
                Expression.Call(
                    null,
                    new Func<IQueryable<Result>, int, int, bool, IAsyncEnumerable<List<Result>>>(BatchAsync<Result>).Method,
                    source.Expression, Expression.Constant(batchSize), Expression.Constant(scrollCacheMinutes), Expression.Constant(useDefaultSort)
                ));
        }
    }

    /// <summary>
    /// Mark this method to be able to convert to IStream from ExpressionNode when executing query. For example : query.ToListAsync() 
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Method)]
    public class StreamQueryCustomMethod_BatchAsyncAttribute : Attribute, Vitorm.StreamQuery.MethodCall.IMethodConvertor
    {
        public IStream Convert(MethodCallConvertArgrument methodConvertArg)
        {
            ExpressionNode_MethodCall call = methodConvertArg.node;
            var reader = methodConvertArg.reader;
            var arg = methodConvertArg.arg;

            if (call.arguments?.Length != 4) return null;
            if (call.methodName != nameof(Queryable_Extensions_BatchAsync.BatchAsync)) return null;

            if (call.arguments[1].value is not int batchSize) batchSize = 5000;
            if (call.arguments[2].value is not int scrollCacheMinutes) scrollCacheMinutes = 1;
            if (call.arguments[3].value is not bool useDefaultSort) useDefaultSort = false;


            var source = reader.ReadStream(arg, call.arguments[0]);
            CombinedStream combinedStream = reader.AsCombinedStream(arg, source);

            combinedStream.method = call.methodName;
            combinedStream.methodArguments = new object[] { batchSize, scrollCacheMinutes, useDefaultSort };

            return combinedStream;
        }
    }
}