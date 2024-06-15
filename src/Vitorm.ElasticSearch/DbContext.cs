using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Vit.Linq.ExpressionTree.ComponentModel;
using Vit.Linq;
using Vit.Core.Module.Serialization;
using System.Net.Http;
using System.Text;
using Vitorm.StreamQuery;
using Vitorm.Entity;
using Vit.Extensions;
using System.Net;
using System.Collections;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext : Vitorm.DbContext
    {
        // https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-bulk.html

        // https://elasticsearch.bookhub.tech/rest_apis/document_apis/reindex


        /// <summary>
        /// es address, example:"http://192.168.20.20:9200"
        /// </summary>
        public string serverAddress { get; set; }

        private System.Net.Http.HttpClient httpClient = null;
        public DbContext(string serverAddress, System.Net.Http.HttpClient httpClient = null)
        {
            this.serverAddress = serverAddress;
            if (httpClient == null)
            {
                // trust all certificate
                var HttpHandler = new HttpClientHandler
                {
                    ServerCertificateCustomValidationCallback = (a, b, c, d) => true
                };
                httpClient = new System.Net.Http.HttpClient(HttpHandler);
            }
            this.httpClient = httpClient;

            this.GetEntityIndex = GetDefaultIndex;
        }


        public virtual string GetIndex<Model>()
        {
            return GetEntityIndex(typeof(Model));
        }


        public virtual Func<Type, string> GetEntityIndex { set; get; }

        public string GetDefaultIndex(Type entityType)
        {
            var entityDescriptor = GetEntityDescriptor(entityType);
            return (entityDescriptor?.tableName ?? entityType.Name).ToLower();
        }


        public Func<IEntityDescriptor, object, string> GetDocumentId = (entityDescriptor, entity) => entityDescriptor.key.GetValue(entity)?.ToString();
        public virtual string Serialize<Model>(Model m)
        {
            return Json.Serialize(m);
        }
        public virtual Model Deserialize<Model>(string jsonString)
        {
            return Json.Deserialize<Model>(jsonString);
        }



      

        #region #1 Schema :  Create

        public override void Create<Entity>()
        {
            var indexName = GetIndex<Entity>();
            Create(indexName);
        }

        public virtual string Create(string indexName, bool throwErrorIfFailed = false)
        {
            var url = $"{serverAddress}/{indexName}";
            var strPayload = "{\"mappings\":{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"time\":{\"type\":\"date\"}}}}";
            var content = new StringContent(strPayload, Encoding.UTF8, "application/json");
            var httpResponse = httpClient.PutAsync(url, content).Result;
            var strResponse = httpResponse.Content.ReadAsStringAsync().Result;
            if (throwErrorIfFailed && !httpResponse.IsSuccessStatusCode) throw new Exception(strResponse);
            return strResponse;
        }
        #endregion


        #region #1 Schema :  Drop

        public virtual void Drop<Entity>()
        {
            var indexName = GetIndex<Entity>();
            Drop(indexName);
        }

        public virtual void Drop(string indexName)
        {
            var url = $"{serverAddress}/{indexName}";
            var httpResponse = httpClient.DeleteAsync(url).Result;

            if (httpResponse.IsSuccessStatusCode) return;

            var strResponse = httpResponse.Content.ReadAsStringAsync().Result;
            if (httpResponse.StatusCode == HttpStatusCode.NotFound && !string.IsNullOrWhiteSpace(strResponse)) return;

            throw new Exception(strResponse);
        }
        #endregion


        // #1 Create :  Add AddRange
        #region Add

        public override Entity Add<Entity>(Entity entity)
        {
            var indexName = GetIndex<Entity>();
            return Add(entity, indexName);
        }
        public virtual Entity Add<Entity>(Entity entity, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var _id = entityDescriptor.key.GetValue(entity) as string;
            var action = string.IsNullOrWhiteSpace(_id) ? "_doc" : "_create";

            return SingleAction(entityDescriptor, entity, indexName, action);
        }



        #endregion


        #region AddRange
        public override void AddRange<Entity>(IEnumerable<Entity> entitys)
        {
            var indexName = GetIndex<Entity>();
            AddRange(entitys, indexName);
        }
        public void AddRange<Entity>(IEnumerable<Entity> entitys, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));
            var bulkResult = Bulk(entityDescriptor, entitys, indexName, "create");

            var items = bulkResult?.items;
            if (items?.Length == entitys.Count())
            {
                var t = 0;
                foreach (var entity in entitys)
                {
                    var id = items[t].result?._id;
                    if (id != null) entityDescriptor.key?.SetValue(entity, id);
                    t++;
                }
            }
        }
        #endregion




        #region #2 Retrieve : Get Query


        #region Get

        public override Entity Get<Entity>(object keyValue)
        {
            var indexName = GetIndex<Entity>();
            return Get<Entity>(keyValue, indexName);
        }
        public virtual Entity Get<Entity>(object keyValue, string indexName)
        {
            var actionUrl = $"{serverAddress}/{indexName}/_doc/" + keyValue;

            var httpResponse = httpClient.GetAsync(actionUrl).Result;

            if (httpResponse.StatusCode == HttpStatusCode.NotFound)
            {
                return default;
            }

            httpResponse.EnsureSuccessStatusCode();

            var strResponse = httpResponse.Content.ReadAsStringAsync().Result;
            var response = Deserialize<GetResult<Entity>>(strResponse);

            if (response.found != true) return default;

            var entity = response._source;
            if (entity != null && response._id != null)
            {
                var entityDescriptor = GetEntityDescriptor(typeof(Entity));
                entityDescriptor.key.SetValue(entity, response._id);
            }
            return entity;
        }

        /// <summary>
        /// result for   GET dev-orm/_doc/3
        /// </summary>
        /// <typeparam name="Entity"></typeparam>
        class GetResult<Entity>
        {
            public string _index { get; set; }
            public string _id { get; set; }

            public string _type { get; set; }
            public int? _version { get; set; }

            public int? _seq_no { get; set; }
            public int? _primary_term { get; set; }
            public bool? found { get; set; }
            public Entity _source { get; set; }
        }
        #endregion      

        #region Query


        public override IQueryable<Entity> Query<Entity>()
        {
            var indexName = GetIndex<Entity>();
            return Query<Entity>(indexName);
        }
        public virtual IQueryable<Entity> Query<Entity>(string indexName)
        {
            var dbContextId = "SqlDbSet_" + GetHashCode();
            Func<Expression, Type, object> QueryExecutor = (expression, type) =>
            {
                // #1 convert to ExpressionNode
                var isArgument = QueryableBuilder.QueryTypeNameCompare(dbContextId);
                ExpressionNode node = convertService.ConvertToData(expression, autoReduce: true, isArgument: isArgument);
                //var strNode = Json.Serialize(node);

                // #2 convert to Stream
                var stream = StreamReader.ReadNode(node);
                //var strStream = Json.Serialize(stream);

                // #3.3 Query
                // #3.3.1
                var combinedStream = stream as CombinedStream;
                if (combinedStream == null) combinedStream = new CombinedStream("tmp") { source = stream };
                SourceStream source = combinedStream.source as SourceStream;

                if (source == null) throw new NotSupportedException("not supported nested query");
                if (combinedStream.isGroupedStream) throw new NotSupportedException("not supported group query");
                if (combinedStream.joins?.Any() == true) throw new NotSupportedException("not supported join query");
                if (combinedStream.distinct != null) throw new NotSupportedException("not supported distinct query");



                var queryPayload = BuildElasticQueryPayload(combinedStream);
                var searchResult = Query<Entity>(queryPayload, indexName);


                if (combinedStream.method == "TotalCount") return searchResult?.hits?.total?.value;
                if (combinedStream.method == "Count") return searchResult?.hits?.hits?.Count() ?? 0;

                var entityDescriptor = GetEntityDescriptor(typeof(Entity));
                var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(entityDescriptor));

                Delegate select = null;
                if (combinedStream.select?.isDefaultSelect == false)
                {
                    select = BuildSelect(source.GetEntityType(), combinedStream.select.fields, source.alias);
                }

                // #3.3.2 execute and read result
                switch (combinedStream.method)
                {
                    case "First": { var entity = entities.First(); return select == null ? entity : select.DynamicInvoke(entity); }
                    case "FirstOrDefault": { var entity = entities.FirstOrDefault(); return select == null ? entity : select.DynamicInvoke(entity); }
                    case "Last": { var entity = entities.Last(); return select == null ? entity : select.DynamicInvoke(entity); }
                    case "LastOrDefault": { var entity = entities.LastOrDefault(); return select == null ? entity : select.DynamicInvoke(entity); }

                    case "ToList":
                    case "":
                    case null:
                        {
                            if (select == null)
                                return entities.ToList();

                            // ToList
                            var resultType = type.GetGenericArguments()[0];
                            var list = Activator.CreateInstance(typeof(List<>).MakeGenericType(resultType)) as IList;
                            foreach (var entity in entities)
                            {
                                list.Add(select.DynamicInvoke(entity));
                            }
                            return list;
                        }
                }
                throw new NotSupportedException("not supported query type: " + combinedStream.method);
            };
            return QueryableBuilder.Build<Entity>(QueryExecutor, dbContextId);
        }

        protected virtual Delegate BuildSelect(Type entityType, ExpressionNode selectedFields, string entityParameterName)
        {
            // Compile Lambda

            var lambdaNode = ExpressionNode.Lambda(new[] { entityParameterName }, selectedFields);
            //var strNode = Json.Serialize(lambdaNode);

            var lambdaExp = convertService.ToLambdaExpression(lambdaNode, new[] { entityType });
            return lambdaExp.Compile();
        }

        protected virtual SearchResponse<Model> Query<Model>(object queryPayload, string indexName)
        {
            var searchUrl = $"{serverAddress}/{indexName}/_search";
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
            = new Dictionary<string, string> { [NodeType.And] = "must", [NodeType.Or] = "should", [NodeType.Not] = "must_not" };
        public virtual object ConvertCondition(ExpressionNode data)
        {
            switch (data.nodeType)
            {
                case NodeType.And:
                case NodeType.Or:
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
                        string optType;
                        switch (operation)
                        {
                            case NodeType.GreaterThan: optType = "gt"; break;
                            case NodeType.GreaterThanOrEqual: optType = "gte"; break;
                            case NodeType.LessThan: optType = "lt"; break;
                            case NodeType.LessThanOrEqual: optType = "lte"; break;
                            default: throw new NotSupportedException("not supported operator:" + operation);
                        }
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
                                    return GetCondition_StringContains(field,   value);
                                }
                            case nameof(string.EndsWith): // String.EndsWith
                                {
                                    ExpressionNode_Member memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(memberNode);
                                    var value = "*" + GetNodeValue(valueNode);
                                    return GetCondition_StringContains(field,   value);
                                }
                            case nameof(string.Contains) when methodCall.methodCall_typeName == "String": // String.Contains
                                {
                                    ExpressionNode_Member memberNode = methodCall.@object;
                                    ExpressionNode valueNode = methodCall.arguments[0];
                                    var field = GetNodeField(memberNode);
                                    var value = "*" + GetNodeValue(valueNode) + "*";
                                    return GetCondition_StringContains(field,  value);
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

        public class SearchResponse<T>
        {
            public HitsContainer hits { get; set; }
            public class HitsContainer
            {
                public List<Hit> hits { get; set; }
                public Total total { get; set; }
                public class Total
                {
                    public int? value { get; set; }
                }
                public class Hit
                {
                    public string _index { get; set; }
                    public string _type { get; set; }
                    public string _id { get; set; }
                    public float? _score { get; set; }
                    public T _source { get; set; }

                    public T GetSource(IEntityDescriptor entityDescriptor)
                    {
                        if (_source != null && _id != null)
                            entityDescriptor?.key?.SetValue(_source, _id);
                        return _source;
                    }
                }
            }
        }

        #endregion

        #endregion



        #region #3 Update: Update UpdateRange
        public override int Update<Entity>(Entity entity)
        {
            return UpdateRange<Entity>(new[] { entity });
        }

        public virtual int Update<Entity>(Entity entity, string indexName)
        {
            return UpdateRange<Entity>(new[] { entity }, indexName);
        }


        public override int UpdateRange<Entity>(IEnumerable<Entity> entitys)
        {
            var indexName = GetIndex<Entity>();
            return UpdateRange<Entity>(entitys, indexName);
        }

        public virtual int UpdateRange<Entity>(IEnumerable<Entity> entitys, string indexName)
        {
            var key = GetEntityDescriptor(typeof(Entity)).key;

            if (entitys.Any(entity => string.IsNullOrWhiteSpace(key.GetValue(entity) as string))) throw new ArgumentNullException("_id");

            SaveRange(entitys);

            return entitys.Count();
        }

        #endregion



        #region Save SaveRange
        public virtual int Save<Entity>(Entity entity)
        {
            var indexName = GetIndex<Entity>();
            return Save<Entity>(entity, indexName);
        }

        public virtual int Save<Entity>(Entity entity, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            //var _id = entityDescriptor.key.GetValue(entity) as string;
            //if (string.IsNullOrWhiteSpace(_id)) throw new ArgumentNullException("_id");

            return SingleAction(entityDescriptor, entity, indexName, "_doc") != null ? 1 : 0;
        }

        public virtual void SaveRange<Entity>(IEnumerable<Entity> entitys)
        {
            var indexName = GetIndex<Entity>();
            SaveRange<Entity>(entitys, indexName);
        }

        public virtual void SaveRange<Entity>(IEnumerable<Entity> entitys, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));
            var bulkResult = Bulk(entityDescriptor, entitys, indexName, "index");

            var items = bulkResult?.items;
            if (items?.Length == entitys.Count())
            {
                var t = 0;
                foreach (var entity in entitys)
                {
                    var id = items[t].result?._id;
                    if (id != null) entityDescriptor.key?.SetValue(entity, id);
                    t++;
                }
            }
        }
        #endregion


        #region #4 Delete : Delete DeleteRange DeleteByKey DeleteByKeys
        public override int Delete<Entity>(Entity entity)
        {
            var indexName = GetIndex<Entity>();
            return Delete<Entity>(entity, indexName);
        }
        public virtual int Delete<Entity>(Entity entity, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var key = entityDescriptor.key.GetValue(entity);
            return DeleteByKey(key, indexName);
        }

        public override int DeleteRange<Entity>(IEnumerable<Entity> entitys)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var keys = entitys.Select(entity => entityDescriptor.key.GetValue(entity)).ToList();
            return DeleteByKeys<Entity, object>(keys);
        }


        public override int DeleteByKey<Entity>(object keyValue)
        {
            var indexName = GetIndex<Entity>();
            return DeleteByKey(keyValue, indexName);
        }
        public virtual int DeleteByKey(object keyValue, string indexName)
        {
            var _id = keyValue?.ToString();

            if (string.IsNullOrWhiteSpace(_id)) throw new ArgumentNullException("_id");

            var actionUrl = $"{serverAddress}/{indexName}/_doc/" + _id;

            var httpResponse = httpClient.DeleteAsync(actionUrl).Result;
            return httpResponse.IsSuccessStatusCode ? 1 : 0;

            //var strResponse = httpResponse.Content.ReadAsStringAsync().Result;
            /*
            {
              "_index": "user",
              "_type": "_doc",
              "_id": "5",
              "_version": 2,
              "result": "deleted",
              "_shards": {
                "total": 2,
                "successful": 1,
                "failed": 0
              },
              "_seq_no": 6,
              "_primary_term": 1
            }
            */
        }
        public override int DeleteByKeys<Entity, Key>(IEnumerable<Key> keys)
        {
            var indexName = GetIndex<Entity>();
            return DeleteByKeys<Entity, Key>(keys, indexName);
        }
        public virtual int DeleteByKeys<Entity, Key>(IEnumerable<Key> keys, string indexName)
        {
            var payload = new StringBuilder();
            foreach (var _id in keys)
            {
                payload.AppendLine($"{{\"delete\":{{\"_index\":\"{indexName}\",\"_id\":\"{_id}\"}}}}");
            }
            var actionUrl = $"{serverAddress}/{indexName}/_bulk";
            var content = new StringContent(payload.ToString(), Encoding.UTF8, "application/json");
            var httpResponse = httpClient.PostAsync(actionUrl, content).Result;

            var strResponse = httpResponse.Content.ReadAsStringAsync().Result;
            if (string.IsNullOrWhiteSpace(strResponse)) httpResponse.EnsureSuccessStatusCode();

            var response = Deserialize<BulkResponse>(strResponse);

            if (response.errors == true)
            {
                var reason = response.items?.FirstOrDefault(m => m.result?.error?.reason != null)?.result?.error?.reason;
                ThrowException(reason, strResponse);
            }

            return response.items.Count(item => item.result?.status == 200);
        }

        #endregion

    }
}