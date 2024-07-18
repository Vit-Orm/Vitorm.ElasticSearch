using System;
using System.Collections.Generic;

using Vit.Linq.ExpressionTree.ExpressionConvertor.MethodCalls;

namespace Vitorm.ElasticSearch
{
    public static partial class NestedField_Extensions
    {

        [CustomMethodAttribute]
        public static T Who<T>(this IEnumerable<T> items) => throw new NotImplementedException();

    }
}