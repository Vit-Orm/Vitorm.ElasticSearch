using System;

using Vit.Linq.ExpressionTree.ExpressionConvertor.MethodCalls;

namespace Vitorm.ElasticSearch
{
    public static partial class String_Extensions
    {

        [CustomMethodAttribute]
        public static bool Like(this string source, string target) => throw new NotImplementedException();

        [CustomMethodAttribute]
        public static bool Match(this string source, string target) => throw new NotImplementedException();
    }
}