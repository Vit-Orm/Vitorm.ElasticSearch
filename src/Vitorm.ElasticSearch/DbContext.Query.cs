using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;

using Vit.Linq.ExpressionTree.ComponentModel;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext
    {

        protected virtual Delegate BuildSelect(Type entityType, ExpressionNode selectedFields, string entityParameterName)
        {
            // Compile Lambda

            var lambdaNode = ExpressionNode.Lambda(new[] { entityParameterName }, selectedFields);
            //var strNode = Json.Serialize(lambdaNode);

            var lambdaExp = convertService.ConvertToCode_LambdaExpression(lambdaNode, new[] { entityType });
            return lambdaExp.Compile();
        }

        protected virtual SearchResponse<Model> Query<Model>(object queryPayload, string indexName)
        {
            var searchUrl = $"{readOnlyServerAddress}/{indexName}/_search";
            var strQuery = Serialize(queryPayload);
            var searchContent = new StringContent(strQuery, Encoding.UTF8, "application/json");
            var httpResponse = httpClient.PostAsync(searchUrl, searchContent).Result;

            var strResponse = httpResponse.Content.ReadAsStringAsync().Result;
            if (!httpResponse.IsSuccessStatusCode) throw new Exception(strResponse);

            var searchResult = Deserialize<SearchResponse<Model>>(strResponse);
            return searchResult;
        }

        public virtual object BuildElasticQueryPayload(CombinedStream combinedStream)
        {
            var queryBody = new Dictionary<string, object>();
            // #1 condition
            var conditionNode = combinedStream.where;
            if (conditionNode == null)
                queryBody["query"] = new { match_all = new { } };
            else
                queryBody["query"] = ConvertCondition(conditionNode);
            // #2 orders
            if (combinedStream.orders?.Any() == true)
            {
                queryBody["sort"] = combinedStream.orders
                                 .Select(order => new Dictionary<string, object> { [GetNodeField(order.member)] = new { order = order.asc ? "asc" : "desc" } })
                                 .ToList();
            }
            // #3 skip take
            if (combinedStream.skip.HasValue)
                queryBody["from"] = combinedStream.skip.Value;
            if (combinedStream.take.HasValue)
                queryBody["size"] = combinedStream.take.Value;
            return queryBody;
        }

        #region ConvertCondition
        public virtual string GetNodeField(ExpressionNode_Member data)
        {
            string parent = null;
            if (data.objectValue?.nodeType == NodeType.Member) parent = GetNodeField(data.objectValue);
            if (parent == null)
                return data?.memberName;
            return parent + "." + data?.memberName;
        }
        public virtual object GetNodeValue(ExpressionNode_Constant data)
        {
            return data?.value;
        }
        static readonly Dictionary<string, string> conditionMap
            = new Dictionary<string, string> { [NodeType.AndAlso] = "must", [NodeType.OrElse] = "should", [NodeType.Not] = "must_not" };
        public virtual object ConvertCondition(ExpressionNode data)
        {
            switch (data.nodeType)
            {
                case NodeType.AndAlso:
                case NodeType.OrElse:
                    {
                        ExpressionNode_Binary binary = data;
                        var condition = conditionMap[data.nodeType];
                        var conditions = new[] { ConvertCondition(binary.left), ConvertCondition(binary.right) };
                        return new { @bool = new Dictionary<string, object> { [condition] = conditions } };
                    }
                case NodeType.Not:
                    {
                        ExpressionNode_Not notNode = data;
                        var condition = conditionMap[data.nodeType];
                        var conditions = new[] { ConvertCondition(notNode.body) };
                        return new { @bool = new Dictionary<string, object> { [condition] = conditions } };
                    }
                case NodeType.NotEqual:
                    {
                        ExpressionNode_Binary binary = data;
                        return ConvertCondition(ExpressionNode.Not(ExpressionNode.Binary(nodeType: NodeType.Equal, left: binary.left, right: binary.right)));
                    }
                case NodeType.Equal:
                    {
                        ExpressionNode_Binary binary = data;
                        ExpressionNode_Member memberNode;
                        ExpressionNode valueNode;
                        string operation = binary.nodeType;
                        if (binary.left.nodeType == NodeType.Member)
                        {
                            memberNode = binary.left;
                            valueNode = binary.right;
                        }
                        else
                        {
                            memberNode = binary.right;
                            valueNode = binary.left;
                        }
                        var field = GetNodeField(memberNode);
                        var value = GetNodeValue(valueNode);

                        // {"term":{"name":"lith" } }
                        return new { term = new Dictionary<string, object> { [field] = value } };
                    }
                case NodeType.LessThan:
                case NodeType.LessThanOrEqual:
                case NodeType.GreaterThan:
                case NodeType.GreaterThanOrEqual:
                    {
                        ExpressionNode_Binary binary = data;
                        ExpressionNode_Member memberNode;
                        ExpressionNode valueNode;
                        string operation = binary.nodeType;
                        if (binary.left.nodeType == NodeType.Member)
                        {
                            memberNode = binary.left;
                            valueNode = binary.right;
                        }
                        else
                        {
                            memberNode = binary.right;
                            valueNode = binary.left;
                            if (operation.StartsWith("LessThan")) operation = operation.Replace("LessThan", "GreaterThan");
                            else operation = operation.Replace("GreaterThan", "LessThan");
                        }
                        var field = GetNodeField(memberNode);
                        var value = GetNodeValue(valueNode);


                        //  { "range": { "age": { "gte": 10, "lte": 20 } } }
                        string optType = operation switch
                        {
                            NodeType.GreaterThan => "gt",
                            NodeType.GreaterThanOrEqual => "gte",
                            NodeType.LessThan => "lt",
                            NodeType.LessThanOrEqual => "lte",
                            _ => throw new NotSupportedException("not supported operator:" + operation),
                        };
                        return new { range = new Dictionary<string, object> { [field] = new Dictionary<string, object> { [optType] = value } } };
                    }
                case NodeType.MethodCall:
                    {
                        ExpressionNode_MethodCall methodCall = data;
                        switch (methodCall.methodName)
                        {
                            #region ##1 String method:  StartsWith EndsWith Contains
                            case nameof(string.StartsWith): // String.StartsWith
                                {
                                    ExpressionNode_Member memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(memberNode);
                                    var value = GetNodeValue(valueNode) + "*";
                                    return GetCondition_StringContains(field, value);
                                }
                            case nameof(string.EndsWith): // String.EndsWith
                                {
                                    ExpressionNode_Member memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(memberNode);
                                    var value = "*" + GetNodeValue(valueNode);
                                    return GetCondition_StringContains(field, value);
                                }
                            case nameof(string.Contains) when methodCall.methodCall_typeName == "String": // String.Contains
                                {
                                    ExpressionNode_Member memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(memberNode);
                                    var value = "*" + GetNodeValue(valueNode) + "*";
                                    return GetCondition_StringContains(field, value);
                                }
                            #endregion

                            // ##2 in
                            case nameof(Enumerable.Contains):
                                {
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    ExpressionNode_Member memberNode = methodCall.arguments[1];
                                    var field = GetNodeField(memberNode);
                                    var value = GetNodeValue(valueNode);

                                    // {"terms":{"name":["lith1","lith2"] } }
                                    return new { terms = new Dictionary<string, object> { [field] = value } };
                                }
                        }
                        break;
                    }
            }
            throw new NotSupportedException("not suported nodeType: " + data.nodeType);
        }
        object GetCondition_StringContains(string field, object value)
        {
            // { "wildcard": { "name.keyword": "*lith*" } }
            return new { wildcard = new Dictionary<string, object> { [field + ".keyword"] = value } };
        }
        #endregion


    }
}
