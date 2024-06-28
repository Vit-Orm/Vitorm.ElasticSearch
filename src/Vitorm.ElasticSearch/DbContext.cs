using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

using Vit.Linq.ExpressionTree.ComponentModel;
using Vit.Linq;
using Vit.Core.Module.Serialization;
using System.Net.Http;
using Vitorm.StreamQuery;
using Vitorm.Entity;
using Vit.Extensions;
using System.Collections;
using Vit.Extensions.Vitorm_Extensions;

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

        protected System.Net.Http.HttpClient httpClient = null;
        protected static System.Net.Http.HttpClient defaultHttpClient = null;
        public DbContext(string serverAddress, System.Net.Http.HttpClient httpClient = null)
        {
            this.serverAddress = serverAddress;
            if (httpClient == null)
            {
                if (defaultHttpClient == null)
                {
                    // trust all certificate
                    var HttpHandler = new HttpClientHandler
                    {
                        ServerCertificateCustomValidationCallback = (a, b, c, d) => true
                    };
                    defaultHttpClient = new System.Net.Http.HttpClient(HttpHandler);
                }
                httpClient = defaultHttpClient;
            }
            this.httpClient = httpClient;

            this.GetEntityIndex = GetDefaultIndex;
        }


        // GetIndex
        public virtual Func<Type, string> GetEntityIndex { set; get; }
        public virtual string GetIndex<Model>()
        {
            return GetEntityIndex(typeof(Model));
        }
        public string GetDefaultIndex(Type entityType)
        {
            var entityDescriptor = GetEntityDescriptor(entityType);
            return (entityDescriptor?.tableName ?? entityType.Name).ToLower();
        }


        // GetDocumentId
        public Func<IEntityDescriptor, object, string> GetDocumentId = (entityDescriptor, entity) => entityDescriptor.key.GetValue(entity)?.ToString();

        // Serialize
        public virtual string Serialize<Model>(Model m)
        {
            return Json.Serialize(m);
        }
        public virtual Model Deserialize<Model>(string jsonString)
        {
            return Json.Deserialize<Model>(jsonString);
        }





        #region #1.1 Schema :  Create

        public override void Create<Entity>()
        {
            var indexName = GetIndex<Entity>();
            Create(indexName);
        }

        public virtual string Create(string indexName, bool throwErrorIfFailed = false)
        {
            return CreateAsync(indexName, throwErrorIfFailed).Result;
        }
        #endregion


        #region #1.2 Schema :  Drop
        public virtual void Drop<Entity>()
        {
            var indexName = GetIndex<Entity>();
            Drop(indexName);
        }

        public virtual void Drop(string indexName)
        {
            DropAsync(indexName).Wait();
        }
        #endregion


        #region #1.1 Create :  Add

        public override Entity Add<Entity>(Entity entity)
        {
            var indexName = GetIndex<Entity>();
            return Add(entity, indexName);
        }
        public virtual Entity Add<Entity>(Entity entity, string indexName)
        {
            return AddAsync(entity, indexName).Result;
        }

        #endregion


        #region #1.2 Create :  AddRange
        public override void AddRange<Entity>(IEnumerable<Entity> entities)
        {
            var indexName = GetIndex<Entity>();
            AddRange(entities, indexName);
        }
        public virtual void AddRange<Entity>(IEnumerable<Entity> entities, string indexName)
        {
            AddRangeAsync(entities, indexName).Wait();
        }
        #endregion


        #region #2.1 Retrieve : Get

        public override Entity Get<Entity>(object keyValue)
        {
            var indexName = GetIndex<Entity>();
            return Get<Entity>(keyValue, indexName);
        }
        public virtual Entity Get<Entity>(object keyValue, string indexName)
        {
            return GetAsync<Entity>(keyValue, indexName).Result;
        }

        #endregion



        #region #2.2 Retrieve : Query
        public override IQueryable<Entity> Query<Entity>()
        {
            var indexName = GetIndex<Entity>();
            return Query<Entity>(indexName);
        }
        public virtual IQueryable<Entity> Query<Entity>(string indexName)
        {
            var dbContextId = "ES_DbSet_" + GetHashCode();
            Func<Expression, Type, object> QueryExecutor = (expression, type) =>
            {
                // #1 convert to ExpressionNode
                var isArgument = QueryableBuilder.CompareQueryByName(dbContextId);
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


                if (combinedStream.method == nameof(Orm_Extensions.ToExecuteString))
                {
                    return Serialize(queryPayload);
                }

                var searchResult = Query<Entity>(queryPayload, indexName);


                if (combinedStream.method == "TotalCount") return searchResult?.hits?.total?.value;
                if (combinedStream.method == "Count") return searchResult?.hits?.hits?.Count() ?? 0;

                var entityDescriptor = GetEntityDescriptor(typeof(Entity));
                var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(entityDescriptor));

                Delegate select = null;
                if (combinedStream.select?.fields != null)
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




        #region #3 Update: Update UpdateRange
        public override int Update<Entity>(Entity entity)
        {
            return UpdateRange<Entity>(new[] { entity });
        }

        public virtual int Update<Entity>(Entity entity, string indexName)
        {
            return UpdateRange<Entity>(new[] { entity }, indexName);
        }


        public override int UpdateRange<Entity>(IEnumerable<Entity> entities)
        {
            var indexName = GetIndex<Entity>();
            return UpdateRange<Entity>(entities, indexName);
        }

        public virtual int UpdateRange<Entity>(IEnumerable<Entity> entities, string indexName)
        {
            return UpdateRangeAsync<Entity>(entities, indexName).Result;
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
            return SaveAsync<Entity>(entity, indexName).Result;
        }

        public virtual void SaveRange<Entity>(IEnumerable<Entity> entities)
        {
            var indexName = GetIndex<Entity>();
            SaveRange<Entity>(entities, indexName);
        }

        public virtual void SaveRange<Entity>(IEnumerable<Entity> entities, string indexName)
        {
            SaveRangeAsync<Entity>(entities, indexName).Wait();
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



        public override int DeleteRange<Entity>(IEnumerable<Entity> entities)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var keys = entities.Select(entity => entityDescriptor.key.GetValue(entity)).ToList();
            return DeleteByKeys<Entity, object>(keys);
        }
        public virtual int DeleteRange<Entity>(IEnumerable<Entity> entities, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var keys = entities.Select(entity => entityDescriptor.key.GetValue(entity)).ToList();
            return DeleteByKeys<Entity, object>(keys, indexName);
        }




        public override int DeleteByKey<Entity>(object keyValue)
        {
            var indexName = GetIndex<Entity>();
            return DeleteByKey(keyValue, indexName);
        }
        public virtual int DeleteByKey(object keyValue, string indexName)
        {
            return DeleteByKeyAsync(keyValue, indexName).Result;
        }



        public override int DeleteByKeys<Entity, Key>(IEnumerable<Key> keys)
        {
            var indexName = GetIndex<Entity>();
            return DeleteByKeys<Entity, Key>(keys, indexName);
        }
        public virtual int DeleteByKeys<Entity, Key>(IEnumerable<Key> keys, string indexName)
        {
            return DeleteByKeysAsync<Entity, Key>(keys, indexName).Result;
        }

        #endregion

    }
}