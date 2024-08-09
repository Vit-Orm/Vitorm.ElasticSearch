using System;
using System.Collections.Generic;
using System.Linq;

namespace Vitorm.ElasticSearch
{
    public static partial class IAsyncEnumerable_Extensions
    {
        public static IEnumerable<T> ToEnumerable<T>(this IAsyncEnumerable<T> asyncEnumerable)
        {
            var asyncEnumerator = asyncEnumerable.GetAsyncEnumerator();
            while (true)
            {
                var hasNext = asyncEnumerator.MoveNextAsync().AsTask().Result;
                if (!hasNext) break;
                yield return asyncEnumerator.Current;
            }
        }


        public static async IAsyncEnumerable<List<Result>> Select<Entity, Result>(this IAsyncEnumerable<List<Entity>> asyncEnumerable, Func<Entity, Result> select)
        {
            await foreach (var items in asyncEnumerable)
            {
                yield return items.Select(select).ToList();
            }
        }

    }
}
