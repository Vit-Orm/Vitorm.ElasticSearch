using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;

using Vit.Linq.ExpressionTree.ComponentModel;

namespace Vitorm.ElasticSearch
{
    public class ExpressionNodeBuilder
    {
        public virtual object ConvertToQuery(ExpressionNode data)
        {
            if (data == null) return new { match_all = new { } };
            return ConvertExpressionNodeToQuery(data);
        }

        public virtual object ConvertExpressionNodeToQuery(ExpressionNode data)
        {
            switch (data.nodeType)
            {
                case NodeType.AndAlso:
                case NodeType.OrElse:
                    {
                        ExpressionNode_Binary binary = data;
                        var condition = conditionMap[data.nodeType];
                        var conditions = new[] { ConvertExpressionNodeToQuery(binary.left), ConvertExpressionNodeToQuery(binary.right) };
                        return new { @bool = new Dictionary<string, object> { [condition] = conditions } };
                    }
                case NodeType.Not:
                    {
                        ExpressionNode_Not notNode = data;
                        var condition = conditionMap[data.nodeType];
                        var conditions = new[] { ConvertExpressionNodeToQuery(notNode.body) };
                        return new { @bool = new Dictionary<string, object> { [condition] = conditions } };
                    }
                case NodeType.NotEqual:
                    {
                        ExpressionNode_Binary binary = data;
                        return ConvertExpressionNodeToQuery(ExpressionNode.Not(ExpressionNode.Binary(nodeType: NodeType.Equal, left: binary.left, right: binary.right)));
                    }
                case NodeType.Equal:
                    {
                        ExpressionNode_Binary binary = data;
                        ExpressionNode memberNode;
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

                        if (memberNode.Member_GetType() == typeof(string))
                        {
                            // {"term":{"name.keyword":"lith" } }
                            return new { term = new Dictionary<string, object> { [field + ".keyword"] = value } };
                        }
                        else
                        {
                            // {"term":{"name":"lith" } }
                            return new { term = new Dictionary<string, object> { [field] = value } };
                        }
                    }
                case NodeType.LessThan:
                case NodeType.LessThanOrEqual:
                case NodeType.GreaterThan:
                case NodeType.GreaterThanOrEqual:
                    {
                        ExpressionNode_Binary binary = data;
                        ExpressionNode memberNode;
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
                                    ExpressionNode memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(memberNode);
                                    var value = GetNodeValue(valueNode) + "*";
                                    return GetCondition_StringLike(field, value);
                                }
                            case nameof(string.EndsWith): // String.EndsWith
                                {
                                    ExpressionNode memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(memberNode);
                                    var value = "*" + GetNodeValue(valueNode);
                                    return GetCondition_StringLike(field, value);
                                }
                            case nameof(string.Contains) when methodCall.methodCall_typeName == "String": // String.Contains
                                {
                                    ExpressionNode memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(memberNode);
                                    var value = "*" + GetNodeValue(valueNode) + "*";
                                    return GetCondition_StringLike(field, value);
                                }
                            #endregion

                            // ##2 Contains
                            case nameof(Enumerable.Contains):
                                {
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    ExpressionNode memberNode = methodCall.arguments[1];
                                    var field = GetNodeField(memberNode);
                                    var value = GetNodeValue(valueNode);

                                    if (memberNode.Member_GetType() == typeof(string))
                                    {
                                        // {"terms":{"name":["lith1","lith2"] } }
                                        return new { terms = new Dictionary<string, object> { [field + ".keyword"] = value } };
                                    }
                                    else
                                    {
                                        // {"terms":{"id":[12,15] } }
                                        return new { terms = new Dictionary<string, object> { [field] = value } };
                                    }
                                }

                            // ##3 String.Like
                            case nameof(String_Extensions.Like):
                                {
                                    ExpressionNode memberNode = methodCall.arguments[0];
                                    ExpressionNode valueNode = methodCall.arguments[1];
                                    var field = GetNodeField(memberNode);
                                    var value = GetNodeValue(valueNode);

                                    // { "wildcard": { "name.keyword": "*lith*" } }
                                    return GetCondition_StringLike(field, value);
                                }

                            // ##4 String.Match
                            case nameof(String_Extensions.Match):
                                {
                                    ExpressionNode memberNode = methodCall.arguments[0];
                                    ExpressionNode valueNode = methodCall.arguments[1];
                                    var field = GetNodeField(memberNode);
                                    var value = GetNodeValue(valueNode);

                                    // { "match": { "name": "lith" } }
                                    return new { match = new Dictionary<string, object> { [field] = value } };
                                }
                        }
                        break;
                    }
            }
            throw new NotSupportedException("not supported nodeType: " + data.nodeType);
        }

        protected virtual object GetCondition_StringLike(string field, object value)
        {
            // { "wildcard": { "name.keyword": "*lith*" } }
            return new { wildcard = new Dictionary<string, object> { [field + ".keyword"] = value } };
        }


        public virtual string GetNodeField(ExpressionNode data)
        {
            if (data?.nodeType == NodeType.MethodCall)
            {
                ExpressionNode_MethodCall methodCall = data;

                //  NestedField
                if (methodCall.methodName == nameof(NestedField_Extensions.Who))
                {
                    ExpressionNode memberNode = methodCall.arguments[0];
                    return GetNodeField(memberNode);
                }
                throw new NotSupportedException("not supported field expression , Node Type: " + data.nodeType);
            }

            if (data?.nodeType != NodeType.Member) throw new NotSupportedException("not supported field expression , Node Type: " + data.nodeType);

            string parent = null;
            Type parentType = null;
            if (data.objectValue != null)
            {
                if (data.objectValue?.nodeType == NodeType.Member)
                {
                    parent = GetNodeField(data.objectValue);
                    parentType = data.objectValue.Member_GetType();
                }
                else if (data.objectValue?.nodeType == NodeType.MethodCall)
                {
                    parent = GetNodeField(data.objectValue);
                    parentType = data.objectValue.MethodCall_GetReturnType();
                }
                else
                    throw new NotSupportedException("not supported field expression , Node Type: " + data.nodeType);
            }

            var memberName = data?.memberName;
            #region Get column defination
            if (memberName != null && parentType != null)
            {
                var field = parentType.GetField(memberName);
                if (field != null)
                {
                    var name = (field.GetCustomAttributes(typeof(ColumnAttribute), true)?.FirstOrDefault() as ColumnAttribute)?.Name;
                    if (name != null) memberName = name;
                }
                else
                {
                    var property = parentType.GetProperty(memberName);
                    if (property != null)
                    {
                        var name = (property.GetCustomAttributes(typeof(ColumnAttribute), true)?.FirstOrDefault() as ColumnAttribute)?.Name;
                        if (name != null) memberName = name;
                    }
                }
            }
            #endregion

            if (parent == null)
                return memberName;
            return parent + "." + memberName;
        }

        public virtual object GetNodeValue(ExpressionNode_Constant data)
        {
            return data?.value;
        }


        public Dictionary<string, string> conditionMap
          = new Dictionary<string, string> { [NodeType.AndAlso] = "filter", [NodeType.OrElse] = "should", [NodeType.Not] = "must_not" };
    }
}
