using System;

using Vit.Linq.ExpressionNodes;

namespace Vitorm.ElasticSearch
{
    public static partial class Object_Extensions_Property
    {

        [ExpressionNode_CustomMethod]
        public static T Property<T>(this object value, string path) => throw new NotImplementedException();

    }
}