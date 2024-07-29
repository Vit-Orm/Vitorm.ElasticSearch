using System;
using System.Collections.Generic;
using System.Linq;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext
    {

        #region #1.0 Schema :  GetMapping
        public virtual String GetMapping<Entity>()
        {
            return GetMappingAsync<Entity>().Result;
        }

        public virtual string GetMapping(string indexName, bool throwErrorIfFailed = false)
        {
            return GetMappingAsync(indexName, throwErrorIfFailed).Result;
        }
        #endregion



        #region #1.1 Schema :  TryCreateTable
        public override void TryCreateTable<Entity>()
        {
            TryCreateTableAsync<Entity>().Wait();
        }

        public virtual string TryCreateTable<Entity>(string indexName, bool throwErrorIfFailed = false)
        {
            return TryCreateTableAsync<Entity>(indexName, throwErrorIfFailed).Result;
        }
        #endregion


        #region #1.2 Schema :  TryDropTable
        public override void TryDropTable<Entity>()
        {
            TryDropTableAsync<Entity>().Wait();
        }

        public virtual void TryDropTable(string indexName)
        {
            TryDropTableAsync(indexName).Wait();
        }
        #endregion


        #region #1.1 Create :  Add

        public override Entity Add<Entity>(Entity entity)
        {
            return AddAsync(entity).Result;
        }
        public virtual Entity Add<Entity>(Entity entity, string indexName)
        {
            return AddAsync(entity, indexName).Result;
        }

        #endregion


        #region #1.2 Create :  AddRange
        public override void AddRange<Entity>(IEnumerable<Entity> entities)
        {
            AddRangeAsync(entities).Wait();
        }
        public virtual void AddRange<Entity>(IEnumerable<Entity> entities, string indexName)
        {
            AddRangeAsync(entities, indexName).Wait();
        }
        #endregion


        #region #2.1 Retrieve : Get

        public override Entity Get<Entity>(object keyValue)
        {
            return GetAsync<Entity>(keyValue).Result;
        }
        public virtual Entity Get<Entity>(object keyValue, string indexName)
        {
            return GetAsync<Entity>(keyValue, indexName).Result;
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

            var key = GetDocumentId(entityDescriptor, entity);
            return DeleteByKey(key, indexName);
        }



        public override int DeleteRange<Entity>(IEnumerable<Entity> entities)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var keys = entities.Select(entity => GetDocumentId(entityDescriptor, entity)).ToList();
            return DeleteByKeys<Entity, object>(keys);
        }
        public virtual int DeleteRange<Entity>(IEnumerable<Entity> entities, string indexName)
        {
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            var keys = entities.Select(entity => GetDocumentId(entityDescriptor, entity)).ToList();
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
