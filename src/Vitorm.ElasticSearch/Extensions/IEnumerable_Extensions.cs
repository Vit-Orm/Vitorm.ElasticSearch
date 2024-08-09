using System.Collections.Generic;

namespace Vitorm.ElasticSearch
{
    public static partial class IEnumerable_Extensions
    {
        public static IEnumerable<T> FlattenEnumerable<T>(this IEnumerable<List<T>> enumerables)
        {
            foreach (var enumerable in enumerables)
            {
                foreach (var item in enumerable)
                {
                    yield return item;
                }
            }
        }

    }
}
