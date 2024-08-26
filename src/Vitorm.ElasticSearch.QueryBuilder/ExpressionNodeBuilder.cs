using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;

using Vit.Linq.ExpressionNodes.ComponentModel;

using Convertor = System.Func<Vitorm.ElasticSearch.QueryBuilder.ExpressionNodeConvertArgument, Vit.Linq.ExpressionNodes.ComponentModel.ExpressionNode, (bool success, object query)>;

namespace Vitorm.ElasticSearch.QueryBuilder
{
    public class ExpressionNodeConvertArgument
    {
        public ExpressionNodeBuilder builder { get; set; }
    }


    public class ExpressionNodeBuilder
    {
        public ExpressionNodeBuilder()
        {
            AddConvertor(String_Extensions.Like_ConvertToQuery, nameof(String_Extensions.Like));
            AddConvertor(String_Extensions.Match_ConvertToQuery, nameof(String_Extensions.Match));
        }



        #region convertors

        public List<(string groupName, Convertor convert)> convertors = new();
        public virtual void AddConvertor(Convertor convertor, string groupName = null)
        {
            convertors.Add((groupName, convertor));
        }
        #endregion



        #region ConvertToQuery

        public virtual object ConvertToQuery(ExpressionNode data)
        {
            if (data == null) return new { match_all = new { } };
            return ConvertExpressionNodeToQuery(new() { builder = this }, data);
        }


        public Dictionary<string, string> conditionTypeMap
          = new() { [NodeType.AndAlso] = "filter", [NodeType.OrElse] = "should", [NodeType.Not] = "must_not" };

        protected virtual object ConvertExpressionNodeToQuery(ExpressionNodeConvertArgument arg, ExpressionNode node)
        {
            switch (node.nodeType)
            {
                case NodeType.Member:
                    {
                        var field = GetNodeField(arg, node, out var fieldType);
                        if (fieldType == typeof(bool) || fieldType == typeof(bool?))
                        {
                            // {"term":{"hasFather": true } }
                            return new { term = new Dictionary<string, object> { [field] = true } };
                        }
                        break;
                    }
                case NodeType.AndAlso:
                case NodeType.OrElse:
                    {
                        ExpressionNode_Binary binary = node;
                        var conditionType = conditionTypeMap[node.nodeType];
                        var condition = new[] { ConvertExpressionNodeToQuery(arg, binary.left), ConvertExpressionNodeToQuery(arg, binary.right) };
                        return new { @bool = new Dictionary<string, object> { [conditionType] = condition } };
                    }
                case NodeType.Not:
                    {
                        ExpressionNode_Not notNode = node;

                        // !bool?.Value
                        if (notNode.body?.nodeType == NodeType.Member)
                        {
                            var field = GetNodeField(arg, notNode.body, out var fieldType);
                            if (fieldType == typeof(bool) || fieldType == typeof(bool?))
                            {
                                // {"term":{"hasFather": false } }
                                return new { term = new Dictionary<string, object> { [field] = false } };
                            }
                        }

                        var conditionType = conditionTypeMap[NodeType.Not];
                        var condition = new[] { ConvertExpressionNodeToQuery(arg, notNode.body) };
                        // {"bool":{"must_not":{"exists":{"field":"address"}}}}
                        return new { @bool = new Dictionary<string, object> { [conditionType] = condition } };
                    }
                case NodeType.NotEqual:
                case NodeType.Equal:
                    {
                        ExpressionNode_Binary binary = node;
                        ExpressionNode memberNode;
                        ExpressionNode valueNode;
                        string operation = binary.nodeType;
                        if (NodeIsField(arg, binary.left))
                        {
                            memberNode = binary.left;
                            valueNode = binary.right;
                        }
                        else
                        {
                            memberNode = binary.right;
                            valueNode = binary.left;
                        }
                        var field = GetNodeField(arg, memberNode, out var fieldType);
                        var value = GetNodeValue(arg, valueNode);

                        if (value == null)
                        {
                            if (node.nodeType == NodeType.NotEqual)
                            {
                                // {"exists":{"field":"address"}}
                                return new { exists = new { field = field } };
                            }
                            else
                            {
                                // {"bool":{"must_not":{"exists":{"field":"address"}}}}
                                return new { @bool = new { must_not = new { exists = new { field = field } } } };

                            }
                        }
                        object condition;
                        if (fieldType == typeof(string))
                        {
                            // {"term":{"name.keyword":"lith" } }
                            condition = new { term = new Dictionary<string, object> { [field + ".keyword"] = value } };
                        }
                        else
                        {
                            // {"term":{"name":"lith" } }
                            condition = new { term = new Dictionary<string, object> { [field] = value } };
                        }

                        if (node.nodeType == NodeType.NotEqual)
                        {
                            var conditionType = conditionTypeMap[NodeType.Not];
                            condition = new { @bool = new Dictionary<string, object> { [conditionType] = condition } };
                        }
                        return condition;
                    }
                case NodeType.LessThan:
                case NodeType.LessThanOrEqual:
                case NodeType.GreaterThan:
                case NodeType.GreaterThanOrEqual:
                    {
                        ExpressionNode_Binary binary = node;
                        ExpressionNode memberNode;
                        ExpressionNode valueNode;
                        string operation = binary.nodeType;
                        if (NodeIsField(arg, binary.left))
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
                        var field = GetNodeField(arg, memberNode, out var fieldType);
                        var value = GetNodeValue(arg, valueNode);


                        //  { "range": { "age": { "gte": 10, "lte": 20 } } }
                        string optType = operation switch
                        {
                            NodeType.GreaterThan => "gt",
                            NodeType.GreaterThanOrEqual => "gte",
                            NodeType.LessThan => "lt",
                            NodeType.LessThanOrEqual => "lte",
                            _ => throw new NotSupportedException("not supported operator:" + operation),
                        };


                        //if (fieldType == typeof(DateTime) || fieldType == typeof(DateTime?))
                        //{
                        //    if (value is DateTime time) value = time.ToString("yyyy-MM-dd HH:mm:ss");
                        //    return new { range = new Dictionary<string, object> { [field] = new Dictionary<string, object> { [optType] = value, ["format"] = "yyyy-MM-dd HH:mm:ss" } } };
                        //}
                        return new { range = new Dictionary<string, object> { [field] = new Dictionary<string, object> { [optType] = value } } };
                    }
                case NodeType.MethodCall:
                    {
                        ExpressionNode_MethodCall methodCall = node;
                        switch (methodCall.methodName)
                        {
                            #region ##1 String method:  StartsWith EndsWith Contains
                            case nameof(string.StartsWith): // String.StartsWith
                                {
                                    ExpressionNode memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(arg, memberNode);
                                    var value = GetNodeValue(arg, valueNode) + "*";
                                    return GetCondition_StringLike(field, value);
                                }
                            case nameof(string.EndsWith): // String.EndsWith
                                {
                                    ExpressionNode memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(arg, memberNode);
                                    var value = "*" + GetNodeValue(arg, valueNode);
                                    return GetCondition_StringLike(field, value);
                                }
                            case nameof(string.Contains) when methodCall.methodCall_typeName == "String": // String.Contains
                                {
                                    ExpressionNode memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(arg, memberNode);
                                    var value = "*" + GetNodeValue(arg, valueNode) + "*";
                                    return GetCondition_StringLike(field, value);
                                }
                            #endregion

                            // ##2 Contains
                            case nameof(Enumerable.Contains):
                                {
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    ExpressionNode memberNode = methodCall.arguments[1];
                                    var field = GetNodeField(arg, memberNode);
                                    var value = GetNodeValue(arg, valueNode);

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
                        }
                        break;
                    }
            }

            foreach (var convertor in convertors)
            {
                var result = convertor.convert(arg, node);
                if (result.success) return result.query;
            }

            throw new NotSupportedException("not supported nodeType: " + node.nodeType);
        }

        static object GetCondition_StringLike(string field, object value)
        {
            // { "wildcard": { "name.keyword": "*lith*" } }
            return new { wildcard = new Dictionary<string, object> { [field + ".keyword"] = value } };
        }
        #endregion


        #region GetNodeField

        public List<string> fieldMethodNames = new()
        {
            nameof(NestedField_Extensions.Who),
            nameof(Object_Extensions_Convert.Convert),
            nameof(Object_Extensions_Property.Property),
        };

        public virtual bool NodeIsField(ExpressionNodeConvertArgument arg, ExpressionNode node)
        {
            if (node.nodeType == NodeType.Member) return true;
            if (node.nodeType == NodeType.MethodCall && fieldMethodNames.Contains(node.methodName))
                return true;
            return false;
        }

        public virtual string GetNodeField(ExpressionNodeConvertArgument arg, ExpressionNode node) => GetNodeField(arg, node, out _);
        public virtual string GetNodeField(ExpressionNode node, out Type type) => GetNodeField(new() { builder = this }, node, out type);

        protected static bool IsNullable(Type type)
        {
            return type?.IsGenericType == true && typeof(Nullable<>) == type.GetGenericTypeDefinition();
        }
        public virtual string GetNodeField(ExpressionNodeConvertArgument arg, ExpressionNode node, out Type type)
        {
            // bool?.Value
            if (node?.nodeType == NodeType.Member && node.objectValue != null && node.memberName == nameof(Nullable<bool>.Value))
            {
                var memberType = node.objectValue.Member_GetType();
                if (IsNullable(memberType))
                {
                    return GetNodeField(arg, node.objectValue, out type);
                }
            }


            if (node?.nodeType == NodeType.MethodCall)
            {
                ExpressionNode_MethodCall methodCall = node;
                type = methodCall.MethodCall_GetReturnType();
                switch (methodCall.methodName)
                {
                    case nameof(NestedField_Extensions.Who):
                        {
                            //  NestedField
                            ExpressionNode child = methodCall.arguments[0];
                            return GetNodeField(arg, child);
                        }
                    case nameof(Object_Extensions_Convert.Convert):
                        {
                            ExpressionNode child = methodCall.arguments[0];
                            return GetNodeField(arg, child);
                        }
                    case nameof(Object_Extensions_Property.Property):
                        {
                            ExpressionNode child = methodCall.arguments[0];
                            ExpressionNode_Constant path = methodCall.arguments[1];
                            var propertyPath = path.value as string;
                            var field = GetNodeField(arg, child);
                            if (string.IsNullOrEmpty(field)) return propertyPath;
                            return $"{field}.{propertyPath}";
                        }
                }
                throw new NotSupportedException("not supported field expression , Node Type: " + node.nodeType);
            }

            if (node?.nodeType != NodeType.Member) throw new NotSupportedException("not supported field expression , Node Type: " + node.nodeType);

            type = node.Member_GetType();

            string parent = null;
            Type parentType = null;
            if (node.objectValue != null)
            {
                if (node.objectValue?.nodeType == NodeType.Member)
                {
                    parent = GetNodeField(arg, node.objectValue);
                    parentType = node.objectValue.Member_GetType();
                }
                else if (node.objectValue?.nodeType == NodeType.MethodCall)
                {
                    parent = GetNodeField(arg, node.objectValue);
                    parentType = node.objectValue.MethodCall_GetReturnType();
                }
                else
                    throw new NotSupportedException("not supported field expression , Node Type: " + node.nodeType);
            }

            var memberName = node?.memberName;
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

        #endregion

        public virtual object GetNodeValue(ExpressionNodeConvertArgument arg, ExpressionNode_Constant data) => data?.value;


    }
}
