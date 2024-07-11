using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

using Vit.Linq;
using Vit.Linq.ExpressionTree.ComponentModel;

using Vitorm.Entity;
using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext
    {

        #region #1.1 Schema :  TryCreateTable

        public override void TryCreateTable<Entity>()
        {
            var indexName = GetIndex<Entity>();
            TryCreateTable(indexName);
        }

        public virtual string TryCreateTable(string indexName, bool throwErrorIfFailed = false)
        {
            return TryCreateTableAsync(indexName, throwErrorIfFailed).Result;
        }
        #endregion


        #region #1.2 Schema :  TryDropTable
        public override void TryDropTable<Entity>()
        {
            var indexName = GetIndex<Entity>();
            TryDropTable(indexName);
        }

        public virtual void TryDropTable(string indexName)
        {
            TryDropTableAsync(indexName).Wait();
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
            return QueryableBuilder.Build<Entity>(QueryExecutor, dbGroupName);

            #region QueryExecutor
            object QueryExecutor(Expression expression, Type expressionResultType)
            {
                // #1 convert to ExpressionNode
                ExpressionNode node = convertService.ConvertToLambdaData(expression, autoReduce: true, isArgument: QueryIsFromSameDb);
                //var strNode = Json.Serialize(node);

                // #2 convert to Stream
                var stream = StreamReader.ReadNode(node);
                //var strStream = Json.Serialize(stream);

                // #3.3 Query
                // #3.3.1
                if (stream is not CombinedStream combinedStream) combinedStream = new CombinedStream("tmp") { source = stream };
                SourceStream source = combinedStream.source as SourceStream ?? throw new NotSupportedException("not supported nested query");
                if (combinedStream.isGroupedStream) throw new NotSupportedException("not supported group query");
                if (combinedStream.joins?.Any() == true) throw new NotSupportedException("not supported join query");
                if (combinedStream.distinct != null) throw new NotSupportedException("not supported distinct query");

                if (combinedStream.method == nameof(Queryable_Extensions.TotalCount) || combinedStream.method == nameof(Queryable.Count))
                {
                    var queryArg = (combinedStream.orders, combinedStream.skip, combinedStream.take);
                    (combinedStream.orders, combinedStream.skip, combinedStream.take) = (null, null, 0);

                    var count = Query<Entity>(BuildElasticQueryPayload(combinedStream), indexName)?.hits?.total?.value ?? 0;

                    if (count > 0 && combinedStream.method == nameof(Queryable.Count))
                    {
                        if (queryArg.skip > 0) count = Math.Max(count - queryArg.skip.Value, 0);

                        if (queryArg.take.HasValue)
                            count = Math.Min(count, queryArg.take.Value);
                    }

                    return count;
                }


                var queryPayload = BuildElasticQueryPayload(combinedStream);

                if (combinedStream.method == nameof(Orm_Extensions.ToExecuteString))
                {
                    return Serialize(queryPayload);
                }

                var searchResult = Query<Entity>(queryPayload, indexName);
          

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
                    case nameof(Queryable.FirstOrDefault): 
                        { 
                            var entity = entities.FirstOrDefault();
                            return select == null ? entity : select.DynamicInvoke(entity);
                        }
                    case nameof(Queryable.First): 
                        { 
                            var entity = entities.First(); 
                            return select == null ? entity : select.DynamicInvoke(entity); 
                        }               
                    case nameof(Queryable.LastOrDefault): 
                        { 
                            var entity = entities.LastOrDefault();
                            return select == null ? entity : select.DynamicInvoke(entity);
                        }
                    case nameof(Queryable.Last):
                        {
                            var entity = entities.Last();
                            return select == null ? entity : select.DynamicInvoke(entity);
                        }
                    case nameof(Queryable_Extensions.ToListAndTotalCount):
                        {
                            IList list; int totalCount;

                            // #1 ToList
                            {
                                if (select == null)
                                {
                                    list = entities.ToList();
                                }
                                else
                                {
                                    // ToList
                                    var resultEntityType = expression.Type.GetGenericArguments()?.FirstOrDefault()?.GetGenericArguments()?.FirstOrDefault();
                                    list = Activator.CreateInstance(typeof(List<>).MakeGenericType(resultEntityType)) as IList;
                                    foreach (var entity in entities)
                                    {
                                        list.Add(select.DynamicInvoke(entity));
                                    }
                                }
                            }

                            // #2 TotalCount
                            totalCount = searchResult?.hits?.total?.value ?? 0;

                            return new Func<object, int, (object, int)>(ValueTuple.Create<object, int>)
                                .Method.GetGenericMethodDefinition()
                                .MakeGenericMethod(list.GetType(), typeof(int))
                                .Invoke(null, new object[] { list, totalCount });

                        }
                    case nameof(Enumerable.ToList):
                    case "":
                    case null:
                        {
                            IList list;

                            if (select == null)
                            {
                                list = entities.ToList();
                            }
                            else
                            {
                                // ToList
                                var resultEntityType = expression.Type.GetGenericArguments()[0];
                                list = Activator.CreateInstance(typeof(List<>).MakeGenericType(resultEntityType)) as IList;
                                foreach (var entity in entities)
                                {
                                    list.Add(select.DynamicInvoke(entity));
                                }
                            }
                            return list;
                        }
                }
                throw new NotSupportedException("not supported query type: " + combinedStream.method);
            }

            #endregion
        }
        /// <summary>
        /// to identify whether contexts are from the same database
        /// </summary>
        protected string dbGroupName { get; set; }
        protected bool QueryIsFromSameDb(object query, Type elementType)
        {
            return dbGroupName == QueryableBuilder.GetQueryConfig(query as IQueryable) as string;
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
