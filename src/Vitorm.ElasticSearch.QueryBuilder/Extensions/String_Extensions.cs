using System;
using System.Collections.Generic;

using Vit.Linq.ExpressionNodes;
using Vit.Linq.ExpressionNodes.ComponentModel;
using Vit.Linq.FilterRules.ComponentModel;

namespace Vitorm.ElasticSearch
{
    public static partial class String_Extensions
    {

        [ExpressionNode_CustomMethod]
        public static bool Like(this string source, string target) => throw new NotImplementedException();

        [ExpressionNode_CustomMethod]
        public static bool Match(this string source, string target) => throw new NotImplementedException();

        #region ExpressionNode
        public static (bool success, object query) Like_ConvertToQuery(ExpressionNodeConvertArgrument arg, ExpressionNode data)
        {
            if (data.nodeType == NodeType.MethodCall && data.methodName == nameof(String_Extensions.Like))
            {
                ExpressionNode_MethodCall methodCall = data;

                ExpressionNode memberNode = methodCall.arguments[0];
                ExpressionNode valueNode = methodCall.arguments[1];
                var field = arg.builder.GetNodeField(arg, memberNode);
                var value = arg.builder.GetNodeValue(arg, valueNode);

                // { "wildcard": { "name.keyword": "*lith*" } }
                var query = new { wildcard = new Dictionary<string, object> { [field + ".keyword"] = value } };
                return (true, query);
            }
            return default;
        }

        public static (bool success, object query) Match_ConvertToQuery(ExpressionNodeConvertArgrument arg, ExpressionNode data)
        {
            if (data.nodeType == NodeType.MethodCall && data.methodName == nameof(String_Extensions.Match))
            {
                ExpressionNode_MethodCall methodCall = data;

                ExpressionNode memberNode = methodCall.arguments[0];
                ExpressionNode valueNode = methodCall.arguments[1];
                var field = arg.builder.GetNodeField(arg, memberNode);
                var value = arg.builder.GetNodeValue(arg, valueNode);

                // { "match": { "name": "lith" } }
                var query = new { match = new Dictionary<string, object> { [field] = value } };
                return (true, query);
            }
            return default;
        }

        #endregion


        #region FilterRule
        public static object Like_ConvertToQuery(FilterRuleConvertArgrument arg, IFilterRule filter, string Operator)
        {
            var field = arg.builder.GetField(arg, filter);
            var value = arg.builder.GetValue(arg, filter);

            // { "wildcard": { "name.keyword": "*lith*" } }
            return new { wildcard = new Dictionary<string, object> { [field + ".keyword"] = value } };
        }
        public static object Match_ConvertToQuery(FilterRuleConvertArgrument arg, IFilterRule filter, string Operator)
        {
            var field = arg.builder.GetField(arg, filter);
            var value = arg.builder.GetValue(arg, filter);

            // { "match": { "name": "lith" } }
            return new { match = new Dictionary<string, object> { [field] = value } };
        }
        #endregion

    }
}