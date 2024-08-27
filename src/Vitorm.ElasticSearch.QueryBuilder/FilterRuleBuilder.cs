using System;
using System.Collections.Generic;
using System.Linq;

using Vit.Linq;
using Vit.Linq.ComponentModel;
using Vit.Linq.FilterRules.ComponentModel;

using Convertor = System.Func<Vitorm.ElasticSearch.QueryBuilder.FilterRuleConvertArgument, Vit.Linq.FilterRules.ComponentModel.IFilterRule, string, object>;

namespace Vitorm.ElasticSearch.QueryBuilder
{
    public class FilterRuleConvertArgument
    {
        public FilterRuleBuilder builder { get; set; }
        public Type entityType { get; set; }

        public int? maxResultWindowSize { get; set; }
        public bool? track_total_hits { get; set; }
    }


    public class FilterRuleBuilder
    {
        public FilterRuleBuilder()
        {
            AddConvertor(nameof(String_Extensions.Like), String_Extensions.Like_ConvertToQuery);
            AddConvertor(nameof(String_Extensions.Match), String_Extensions.Match_ConvertToQuery);
        }

        public virtual Dictionary<string, object> ConvertToQueryPayload<Entity>(RangedQuery query, int maxResultWindowSize = 10000, bool track_total_hits = false)
        {
            var arg = new FilterRuleConvertArgument() { builder = this, maxResultWindowSize = maxResultWindowSize, track_total_hits = track_total_hits };
            arg.entityType = typeof(Entity);
            return ConvertToQueryPayload(arg, query);
        }

        public virtual Dictionary<string, object> ConvertToQueryPayload(RangedQuery query, int maxResultWindowSize = 10000, bool track_total_hits = false)
        {
            var arg = new FilterRuleConvertArgument() { builder = this, maxResultWindowSize = maxResultWindowSize, track_total_hits = track_total_hits };
            return ConvertToQueryPayload(arg, query);
        }


        public virtual Dictionary<string, object> ConvertToQueryPayload(FilterRuleConvertArgument arg, RangedQuery query)
        {
            var queryBody = new Dictionary<string, object>();

            // #1 where
            queryBody["query"] = ConvertToQuery(arg, query.filter);

            // #2 orders
            if (query.orders?.Any() == true)
            {
                queryBody["sort"] = ConvertSort(arg, query.orders);
            }

            // #3 skip take
            int skip = 0;
            if (query.range?.skip > 0)
                queryBody["from"] = skip = query.range.skip;

            var maxResultWindowSize = arg.maxResultWindowSize ?? 10000;
            var take = query.range?.take >= 0 ? query.range.take : maxResultWindowSize;
            if (take + skip > maxResultWindowSize) take = maxResultWindowSize - skip;
            queryBody["size"] = take;


            // #4 track_total_hits
            if (arg.track_total_hits == true) queryBody["track_total_hits"] = true;

            return queryBody;
        }

        public virtual object ConvertSort(FilterRuleConvertArgument arg, List<OrderField> orders)
        {
            if (arg.entityType == null)
            {
                return orders.Select(order => new Dictionary<string, object> { [order.field] = new { order = order.asc ? "asc" : "desc" } }).ToList();
            }

            return orders.Select(order =>
            {
                var field = order.field;
                var fieldType = GetFieldType(arg, field);
                if (fieldType == typeof(string) && !field.EndsWith(".keyword"))
                    field += ".keyword";

                return new Dictionary<string, object> { [field] = new { order = order.asc ? "asc" : "desc" } };
            })
                .ToList();
        }

        public virtual object ConvertToQuery<Entity>(IFilterRule filter)
        {
            return ConvertToQuery(new() { builder = this, entityType = typeof(Entity) }, filter);
        }
        public virtual object ConvertToQuery(IFilterRule filter)
        {
            return ConvertToQuery(new() { builder = this }, filter);
        }

        public virtual object ConvertToQuery(FilterRuleConvertArgument arg, IFilterRule filter)
        {
            if (filter == null) return new { match_all = new { } };
            return ConvertFilterToQuery(arg, filter);
        }

        public virtual object ConvertFilterToQuery(FilterRuleConvertArgument arg, IFilterRule filter) => ConvertConditionToQuery(arg, filter, GetRuleCondition(filter));

        public bool operatorIsIgnoreCase = true;
        public StringComparison comparison => operatorIsIgnoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        protected Dictionary<string, string> operatorMap = new();

        public virtual FilterRuleBuilder AddOperatorMap(string operatorName, string operatorType)
        {
            if (operatorIsIgnoreCase) operatorName = operatorName.ToLower();
            operatorMap[operatorName] = operatorType;
            return this;
        }

        protected virtual string GetOperator(IFilterRule filter)
        {
            var Operator = filter.@operator ?? "";
            if (operatorIsIgnoreCase) Operator = Operator.ToLower();
            if (operatorMap.TryGetValue(Operator, out var value)) return operatorIsIgnoreCase ? value?.ToLower() : value;
            return Operator;
        }


        public Dictionary<string, string> conditionTypeMap
                = new() { [RuleCondition.And] = "filter", [RuleCondition.Or] = "should", [RuleCondition.Not] = "must_not" };

        protected virtual string GetRuleCondition(IFilterRule filter)
        {
            if (!operatorIsIgnoreCase) return filter.condition;

            if (RuleCondition.And.Equals(filter.condition, comparison)) return RuleCondition.And;
            if (RuleCondition.Or.Equals(filter.condition, comparison)) return RuleCondition.Or;
            if (RuleCondition.Not.Equals(filter.condition, comparison)) return RuleCondition.Not;
            if (RuleCondition.NotAnd.Equals(filter.condition, comparison)) return RuleCondition.NotAnd;
            if (RuleCondition.NotOr.Equals(filter.condition, comparison)) return RuleCondition.NotOr;

            return filter.condition;
        }
        public virtual string GetField(FilterRuleConvertArgument arg, IFilterRule filter) => filter?.field;
        public virtual string GetField(FilterRuleConvertArgument arg, IFilterRule filter, out Type fieldType)
        {
            var field = GetField(arg, filter);
            fieldType = GetFieldType(arg, field);
            return field;
        }
        public virtual Type GetFieldType(FilterRuleConvertArgument arg, string field) => LinqHelp.GetNestedMemberType(arg.entityType, field);

        public virtual object GetValue(FilterRuleConvertArgument arg, IFilterRule filter) => filter?.value;

        public virtual object ConvertConditionToQuery(FilterRuleConvertArgument arg, IFilterRule filter, string condition)
        {
            switch (condition)
            {
                case RuleCondition.And:
                case RuleCondition.Or:
                case RuleCondition.NotAnd:
                    {
                        var conditionType = conditionTypeMap[condition];
                        var conditions = filter.rules?.Select(filter => ConvertToQuery(arg, filter)).ToArray();
                        return new { @bool = new Dictionary<string, object> { [conditionType] = conditions } };
                    }
                case RuleCondition.Not:
                    {
                        var conditionType = conditionTypeMap[condition];
                        var conditions = filter.rules?.Select(filter => ConvertToQuery(arg, filter)).ToArray();
                        conditions ??= new[] { ConvertConditionToQuery(arg, filter, null) };
                        return new { @bool = new Dictionary<string, object> { [conditionType] = conditions } };
                    }
                case RuleCondition.NotOr:
                    {
                        var conditionType = conditionTypeMap[RuleCondition.Not];
                        var conditions = filter.rules?.Select(filter => ConvertConditionToQuery(arg, filter, RuleCondition.Or)).ToArray();
                        return new { @bool = new Dictionary<string, object> { [conditionType] = conditions } };
                    }
            }
            var Operator = GetOperator(filter);
            return ConvertOperatorToQuery(arg, filter, Operator);
        }

        #region OperatorConvertor

        public List<(string Operator, Convertor convertor)> operatorConvertors = new() {
            // Equal
            (RuleOperator.Equal,OperatorConvertor_Equal),
            (RuleOperator.NotEqual,OperatorConvertor_NotEqual),

            // IsNull
            (RuleOperator.IsNull,OperatorConvertor_IsNull),
            (RuleOperator.IsNotNull,OperatorConvertor_IsNotNull),

            // Compare
            (RuleOperator.GreaterThan,OperatorConvertor_Compare),
            (RuleOperator.GreaterThanOrEqual,OperatorConvertor_Compare),
            (RuleOperator.LessThan,OperatorConvertor_Compare),
            (RuleOperator.LessThanOrEqual,OperatorConvertor_Compare),

            // In
            (RuleOperator.In,OperatorConvertor_In),
            (RuleOperator.NotIn,OperatorConvertor_NotIn),

            // String
            (RuleOperator.Contains,OperatorConvertor_String),
            (RuleOperator.StartsWith,OperatorConvertor_String),
            (RuleOperator.EndsWith,OperatorConvertor_String),
        };

        public virtual void AddConvertor(string Operator, Convertor convertor)
        {
            operatorConvertors.Add((Operator, convertor));
        }


        #endregion

        public virtual object ConvertOperatorToQuery(FilterRuleConvertArgument arg, IFilterRule filter, string Operator)
        {
            var convertor = operatorConvertors.FirstOrDefault(item => Operator?.Equals(item.Operator, comparison) == true).convertor;
            if (convertor != null) return convertor(arg, filter, Operator);

            if (Operator.StartsWith("Not", comparison))
            {
                var conditions = ConvertOperatorToQuery(arg, filter, Operator.Substring(3));
                return OperatorConvertor_Not(conditions);
            }
            else if (Operator.StartsWith("!", comparison))
            {
                var conditions = ConvertOperatorToQuery(arg, filter, Operator.Substring(1));
                return OperatorConvertor_Not(conditions);
            }

            throw new NotSupportedException("not supported Operator: " + Operator);
        }

        public static object OperatorConvertor_Not(object condition)
        {
            return new { @bool = new Dictionary<string, object> { ["must_not"] = condition } };
        }

        #region OperatorConvertor
        public static object OperatorConvertor_Equal(FilterRuleConvertArgument arg, IFilterRule filter, string Operator)
        {
            Type valueType;
            var field = arg.builder.GetField(arg, filter, out valueType);
            var value = arg.builder.GetValue(arg, filter);

            object query;

            // == null
            if (value == null)
            {
                // {"bool":{"must_not":{"exists":{"field":"address"}}}}
                query = new { exists = new { field } };
                query = OperatorConvertor_Not(query);
                return query;
            }

            valueType ??= value.GetType();
            if (valueType == typeof(string))
            {
                // {"term":{"name.keyword":"lith" } }
                query = new { term = new Dictionary<string, object> { [field + ".keyword"] = value } };
            }
            else
            {
                // {"term":{"name":"lith" } }
                query = new { term = new Dictionary<string, object> { [field] = value } };
            }
            return query;
        }
        public static object OperatorConvertor_NotEqual(FilterRuleConvertArgument arg, IFilterRule filter, string Operator)
        {
            Type valueType;
            var field = arg.builder.GetField(arg, filter, out valueType);
            var value = arg.builder.GetValue(arg, filter);

            object query;

            // == null
            if (value == null)
            {
                // {"bool":{"must_not":{"exists":{"field":"address"}}}}
                query = new { exists = new { field } };
                return query;
            }
            valueType ??= value.GetType();
            if (valueType == typeof(string))
            {
                // {"term":{"name.keyword":"lith" } }
                query = new { term = new Dictionary<string, object> { [field + ".keyword"] = value } };
            }
            else
            {
                // {"term":{"name":"lith" } }
                query = new { term = new Dictionary<string, object> { [field] = value } };
            }
            query = OperatorConvertor_Not(query);
            return query;
        }

        public static object OperatorConvertor_IsNotNull(FilterRuleConvertArgument arg, IFilterRule filter, string Operator)
        {
            var field = arg.builder.GetField(arg, filter, out var valueType);

            // {"exists":{"field":"address"}}
            return new { exists = new { field } };
        }
        public static object OperatorConvertor_IsNull(FilterRuleConvertArgument arg, IFilterRule filter, string Operator)
            => OperatorConvertor_Not(OperatorConvertor_IsNotNull(arg, filter, Operator));



        public static object OperatorConvertor_Compare(FilterRuleConvertArgument arg, IFilterRule filter, string Operator)
        {
            var field = arg.builder.GetField(arg, filter, out var valueType);
            var value = arg.builder.GetValue(arg, filter);

            //  { "range": { "age": { "gte": 10, "lte": 20 } } }
            string optType = Operator switch
            {
                RuleOperator.GreaterThan => "gt",
                RuleOperator.GreaterThanOrEqual => "gte",
                RuleOperator.LessThan => "lt",
                RuleOperator.LessThanOrEqual => "lte",
                _ => throw new NotSupportedException("not supported operator:" + Operator),
            };
            return new { range = new Dictionary<string, object> { [field] = new Dictionary<string, object> { [optType] = value } } };
        }


        public static object OperatorConvertor_In(FilterRuleConvertArgument arg, IFilterRule filter, string Operator)
        {
            var field = arg.builder.GetField(arg, filter, out var valueType);
            var value = arg.builder.GetValue(arg, filter);

            valueType ??= LinqHelp.GetElementType(value.GetType());
            if (valueType == typeof(string))
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
        public static object OperatorConvertor_NotIn(FilterRuleConvertArgument arg, IFilterRule filter, string Operator)
            => OperatorConvertor_Not(OperatorConvertor_In(arg, filter, Operator));


        public static object OperatorConvertor_String(FilterRuleConvertArgument arg, IFilterRule filter, string Operator)
        {
            var field = arg.builder.GetField(arg, filter, out var valueType);
            var value = arg.builder.GetValue(arg, filter);

            if (RuleOperator.Contains.Equals(Operator, arg.builder.comparison))
            {
                value = "*" + value + "*";
            }
            else if (RuleOperator.StartsWith.Equals(Operator, arg.builder.comparison))
            {
                value = value + "*";
            }
            else if (RuleOperator.EndsWith.Equals(Operator, arg.builder.comparison))
            {
                value = "*" + value;
            }

            // { "wildcard": { "name.keyword": "*lith*" } }
            return GetCondition_StringLike(field, value);
        }

        public static object GetCondition_StringLike(string field, object value)
        {
            // { "wildcard": { "name.keyword": "*lith*" } }
            return new { wildcard = new Dictionary<string, object> { [field + ".keyword"] = value } };
        }
        #endregion



    }
}
