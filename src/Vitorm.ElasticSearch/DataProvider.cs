using System.Collections.Generic;
using System.Linq;

using Vitorm.DataProvider;

namespace Vitorm.ElasticSearch
{
    public class DataProvider : IDataProvider
    {
        Vitorm.DbContext IDataProvider.CreateDbContext() => this.CreateDbContext();

        protected Dictionary<string, object> config;
        protected string connectionString;
        protected int? commandTimeout;
        protected Vitorm.ElasticSearch.DbContext dbContext;
        public DbContext CreateDbContext() => dbContext ??= new Vitorm.ElasticSearch.DbContext(serverAddress: connectionString, commandTimeout: commandTimeout);

        public void Init(Dictionary<string, object> config)
        {
            this.config = config;

            if (config.TryGetValue("connectionString", out var connStr))
                this.connectionString = connStr as string;

            if (config.TryGetValue("commandTimeout", out var strCommandTimeout) && int.TryParse("" + strCommandTimeout, out var commandTimeout))
                this.commandTimeout = commandTimeout;
        }


        // #0 Schema :  Create
        public virtual void Create<Entity>() => CreateDbContext().Create<Entity>();
        public virtual void Drop<Entity>() => CreateDbContext().Drop<Entity>();


        // #1 Create :  Add AddRange
        public virtual Entity Add<Entity>(Entity entity) => CreateDbContext().Add<Entity>(entity);
        public virtual void AddRange<Entity>(IEnumerable<Entity> entities) => CreateDbContext().AddRange<Entity>(entities);

        // #2 Retrieve : Get Query
        public virtual Entity Get<Entity>(object keyValue) => CreateDbContext().Get<Entity>(keyValue);
        public virtual IQueryable<Entity> Query<Entity>() => CreateDbContext().Query<Entity>();


        // #3 Update: Update UpdateRange
        public virtual int Update<Entity>(Entity entity) => CreateDbContext().Update<Entity>(entity);
        public virtual int UpdateRange<Entity>(IEnumerable<Entity> entities) => CreateDbContext().UpdateRange<Entity>(entities);


        // #4 Delete : Delete DeleteRange DeleteByKey DeleteByKeys
        public virtual int Delete<Entity>(Entity entity) => CreateDbContext().Delete<Entity>(entity);
        public virtual int DeleteRange<Entity>(IEnumerable<Entity> entities) => CreateDbContext().DeleteRange<Entity>(entities);

        public virtual int DeleteByKey<Entity>(object keyValue) => CreateDbContext().DeleteByKey<Entity>(keyValue);
        public virtual int DeleteByKeys<Entity, Key>(IEnumerable<Key> keys) => CreateDbContext().DeleteByKeys<Entity, Key>(keys);

    }
}
