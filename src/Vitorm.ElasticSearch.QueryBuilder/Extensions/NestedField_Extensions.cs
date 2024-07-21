using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Linq;
using System.Reflection;

using Vit.Linq.ExpressionTree.ExpressionConvertor.MethodCalls;

namespace Vitorm.ElasticSearch
{
    public static partial class NestedField_Extensions
    {

        [CustomMethodAttribute]
        public static T Who<T>(this IEnumerable<T> items) => throw new NotImplementedException();



        #region Method cache
        private static MethodInfo MethodInfo_Who_;
        public static MethodInfo MethodInfo_Who(Type entityType) =>
             (MethodInfo_Who_ ??=
                  new Func<IEnumerable<object>, object>(Who)
                 .GetMethodInfo().GetGenericMethodDefinition())
             .MakeGenericMethod(entityType);
        #endregion



        #region GetFieldExpression

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parameter"></param>
        /// <param name="fieldPath"> could be nested , example: "name"  "depart.name"  "departs[1].name" "departs.1.name"</param>
        /// <returns></returns>
        public static Expression GetFieldExpression(Expression parameter, string fieldPath)
        {
            if (string.IsNullOrWhiteSpace(fieldPath)) return parameter;

            fieldPath = fieldPath.Replace("]", "").Replace("[", ".");
            foreach (var fieldName in fieldPath.Split('.'))
            {
                parameter = GetFieldExpression_ByName(parameter, fieldName);
            }
            return parameter;
        }
        public static Expression GetFieldExpression_ByName(Expression parameter, string propertyOrFieldName)
        {
            var valueType = parameter.Type;

            if (valueType.IsArray)
            {
                // Array
                if (int.TryParse(propertyOrFieldName, out var index))
                    return Expression.ArrayAccess(parameter, Expression.Constant(index));

                var field = Expression.Call(null, NestedField_Extensions.MethodInfo_Who(valueType.GetElementType()), parameter);
                return GetFieldExpression_ByName(field, propertyOrFieldName);
            }
            else if (valueType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(valueType))
            {
                // IEnumerable<>    List<>
                if (int.TryParse(propertyOrFieldName, out var index))
                    return Expression.Call(typeof(Enumerable), "ElementAt", valueType.GetGenericArguments(), parameter, Expression.Constant(index));


                var field = Expression.Call(null, NestedField_Extensions.MethodInfo_Who(valueType.GetGenericArguments()[0]), parameter);
                return GetFieldExpression_ByName(field, propertyOrFieldName);
            }

            return Expression.PropertyOrField(parameter, propertyOrFieldName);
        }
        #endregion


    }
}