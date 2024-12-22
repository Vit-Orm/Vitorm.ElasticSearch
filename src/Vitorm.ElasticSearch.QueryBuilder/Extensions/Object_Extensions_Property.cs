using System;

using Vit.Linq.ExpressionNodes;

namespace Vitorm.ElasticSearch
{
    public static partial class Object_Extensions_Property
    {

        /// <summary>
        /// ElasticSearch server side fieldPath
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <param name="path"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        [ExpressionNode_CustomMethod]
        public static T Property<T>(this object value, string path) => throw new NotImplementedException();

    }
}