using System;
using System.Collections.Generic;

using Vit.Linq.ExpressionTree.ComponentModel;
using Vit.Linq.ExpressionTree.ExpressionConvertor.MethodCalls;
using Vit.Linq.Filter.ComponentModel;

namespace Vitorm.ElasticSearch
{
    public static partial class String_Extensions
    {

        [CustomMethodAttribute]
        public static bool Like(this string source, string target) => throw new NotImplementedException();

        [CustomMethodAttribute]
        public static bool Match(this string source, string target) => throw new NotImplementedException();

        #region ExpressionNode
        public static (bool success, object query) Like_ConvertToQuery(ExpressionNodeBuilder builder, ExpressionNode data)
        {
            if (data.nodeType == NodeType.MethodCall && data.methodName == nameof(String_Extensions.Like))
            {
                ExpressionNode_MethodCall methodCall = data;

                ExpressionNode memberNode = methodCall.arguments[0];
                ExpressionNode valueNode = methodCall.arguments[1];
                var field = builder.GetNodeField(memberNode);
                var value = builder.GetNodeValue(valueNode);

                // { "wildcard": { "name.keyword": "*lith*" } }
                var query = new { wildcard = new Dictionary<string, object> { [field + ".keyword"] = value } };
                return (true, query);
            }
            return default;
        }

        public static (bool success, object query) Match_ConvertToQuery(ExpressionNodeBuilder builder, ExpressionNode data)
        {
            if (data.nodeType == NodeType.MethodCall && data.methodName == nameof(String_Extensions.Match))
            {
                ExpressionNode_MethodCall methodCall = data;

                ExpressionNode memberNode = methodCall.arguments[0];
                ExpressionNode valueNode = methodCall.arguments[1];
                var field = builder.GetNodeField(memberNode);
                var value = builder.GetNodeValue(valueNode);

                // { "match": { "name": "lith" } }
                var query = new { match = new Dictionary<string, object> { [field] = value } };
                return (true, query);
            }
            return default;
        }

        #endregion


        #region FilterRule
        public static object Like_ConvertToQuery(FilterRuleBuilder builder, IFilterRule filter, string Operator)
        {
            var field = builder.GetField(filter);
            var value = builder.GetValue(filter);

            // { "wildcard": { "name.keyword": "*lith*" } }
            return new { wildcard = new Dictionary<string, object> { [field + ".keyword"] = value } };
        }
        public static object Match_ConvertToQuery(FilterRuleBuilder builder, IFilterRule filter, string Operator)
        {
            var field = builder.GetField(filter);
            var value = builder.GetValue(filter);

            // { "match": { "name": "lith" } }
            return new { match = new Dictionary<string, object> { [field] = value } };
        }
        #endregion

    }
}