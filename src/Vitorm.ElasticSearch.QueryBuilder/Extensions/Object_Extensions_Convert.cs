using System;

using Vit.Linq.ExpressionNodes;

namespace Vitorm.ElasticSearch
{
    public static partial class Object_Extensions_Convert
    {

        [ExpressionNode_CustomMethod]
        public static T Convert<T>(this object value) => throw new NotImplementedException();


    }
}