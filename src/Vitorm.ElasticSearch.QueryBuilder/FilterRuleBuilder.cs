using System;
using System.Collections.Generic;
using System.Linq;

using Vit.Linq.Filter.ComponentModel;

namespace Vitorm.ElasticSearch
{
    public class FilterRuleBuilder
    {
        public virtual object ConvertToQuery(IFilterRule filter)
        {
            if (filter == null) return new { match_all = new { } };
            return ConvertFilterToQuery(filter);
        }

        public virtual object ConvertFilterToQuery(IFilterRule filter) => ConvertConditionToQuery(filter, GetRuleCondition(filter));

        public bool operatorIsIgnoreCase = true;
        protected StringComparison comparison => operatorIsIgnoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        protected Dictionary<string, string> operatorMap = new Dictionary<string, string>();

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


        public Dictionary<string, string> conditionMap
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
        protected virtual string GetField(IFilterRule filter) => filter?.field;
        protected virtual object GetValue(IFilterRule filter) => filter?.value;

        public virtual object ConvertConditionToQuery(IFilterRule filter, string condition)
        {
            switch (condition)
            {
                case RuleCondition.And:
                case RuleCondition.Or:
                case RuleCondition.NotAnd:
                    {
                        var conditionType = conditionMap[condition];
                        var conditions = filter.rules?.Select(ConvertToQuery).ToArray();
                        return new { @bool = new Dictionary<string, object> { [conditionType] = conditions } };
                    }
                case RuleCondition.Not:
                    {
                        var conditionType = conditionMap[condition];
                        var conditions = filter.rules?.Select(ConvertToQuery).ToArray();
                        if (conditions == null) conditions = new[] { ConvertConditionToQuery(filter, null) };
                        return new { @bool = new Dictionary<string, object> { [conditionType] = conditions } };
                    }
                case RuleCondition.NotOr:
                    {
                        var conditionType = conditionMap[RuleCondition.Not];
                        var conditions = filter.rules?.Select(filter => ConvertConditionToQuery(filter, RuleCondition.Or)).ToArray();
                        return new { @bool = new Dictionary<string, object> { [conditionType] = conditions } };
                    }
            }
            var Operator = GetOperator(filter);
            return ConvertOperatorToQuery(filter, Operator);
        }

        public List<(string Operator, Func<FilterRuleBuilder, IFilterRule, string, object> convertor)> operatorConvertors = new() {
            // Equal
            (RuleOperator.Equal,OperatorConvertor_Equal),
            (RuleOperator.NotEqual,OperatorConvertor_NotEqual),

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
            (nameof(String_Extensions.Like),OperatorConvertor_String),
            (nameof(String_Extensions.Match),OperatorConvertor_String),
        };


        public virtual object ConvertOperatorToQuery(IFilterRule filter, string Operator)
        {
            var convertor = operatorConvertors.FirstOrDefault(item => Operator?.Equals(item.Operator, comparison) == true).convertor;
            if (convertor != null) return convertor(this, filter, Operator);

            if (Operator.StartsWith("Not", comparison))
            {
                var conditions = ConvertOperatorToQuery(filter, Operator.Substring(3));
                return OperatorConvertor_Not(conditions);
            }
            else if (Operator.StartsWith("!", comparison))
            {
                var conditions = ConvertOperatorToQuery(filter, Operator.Substring(1));
                return OperatorConvertor_Not(conditions);
            }

            throw new NotSupportedException("not supported Operator: " + Operator);
        }

        public static object OperatorConvertor_Not(object condition)
        {
            return new { @bool = new Dictionary<string, object> { ["must_not"] = condition } };
        }

        #region OperatorConvertor
        public static object OperatorConvertor_Equal(FilterRuleBuilder builder, IFilterRule filter, string Operator)
        {
            var field = builder.GetField(filter);
            var value = builder.GetValue(filter);

            if (value.GetType() == typeof(string))
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
        public static object OperatorConvertor_NotEqual(FilterRuleBuilder builder, IFilterRule filter, string Operator) => OperatorConvertor_Not(OperatorConvertor_Equal(builder, filter, Operator));
        public static object OperatorConvertor_NotIn(FilterRuleBuilder builder, IFilterRule filter, string Operator) => OperatorConvertor_Not(OperatorConvertor_In(builder, filter, Operator));

        public static object OperatorConvertor_Compare(FilterRuleBuilder builder, IFilterRule filter, string Operator)
        {
            var field = builder.GetField(filter);
            var value = builder.GetValue(filter);

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
        public static object OperatorConvertor_In(FilterRuleBuilder builder, IFilterRule filter, string Operator)
        {
            var field = builder.GetField(filter);
            var value = builder.GetValue(filter);

            if (value?.GetType() == typeof(string))
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
        public static object OperatorConvertor_String(FilterRuleBuilder builder, IFilterRule filter, string Operator)
        {
            var field = builder.GetField(filter);
            var value = builder.GetValue(filter);

            if (nameof(String_Extensions.Match).Equals(Operator, builder.comparison))
            {
                // { "match": { "name": "lith" } }
                return new { match = new Dictionary<string, object> { [field] = value } };
            }

            if (nameof(String_Extensions.Like).Equals(Operator, builder.comparison))
            {
            }
            else if (RuleOperator.Contains.Equals(Operator, builder.comparison))
            {
                value = "*" + value + "*";
            }
            else if (RuleOperator.StartsWith.Equals(Operator, builder.comparison))
            {
                value = value + "*";
            }
            else if (RuleOperator.EndsWith.Equals(Operator, builder.comparison))
            {
                value = "*" + value;
            }

            // { "wildcard": { "name.keyword": "*lith*" } }
            return builder.GetCondition_StringLike(field, value);

        }
        protected virtual object GetCondition_StringLike(string field, object value)
        {
            // { "wildcard": { "name.keyword": "*lith*" } }
            return new { wildcard = new Dictionary<string, object> { [field + ".keyword"] = value } };
        }

        #endregion



    }
}
