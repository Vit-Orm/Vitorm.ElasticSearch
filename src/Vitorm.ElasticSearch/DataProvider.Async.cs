using System.Collections.Generic;
using System.Threading.Tasks;

using Vitorm.DataProvider;

namespace Vitorm.ElasticSearch
{
    public partial class DataProvider : IDataProvider
    {
        // #0 Schema :  Create
        public virtual Task TryCreateTableAsync<Entity>() => CreateDbContext().TryCreateTableAsync<Entity>();
        public virtual Task TryDropTableAsync<Entity>() => CreateDbContext().TryDropTableAsync<Entity>();
        public virtual Task TruncateAsync<Entity>() => CreateDbContext().TruncateAsync<Entity>();


        // #1 Create :  Add AddRange
        public virtual Task<Entity> AddAsync<Entity>(Entity entity) => CreateDbContext().AddAsync<Entity>(entity);
        public virtual Task AddRangeAsync<Entity>(IEnumerable<Entity> entities) => CreateDbContext().AddRangeAsync<Entity>(entities);

        // #2 Retrieve : Get Query
        public virtual Task<Entity> GetAsync<Entity>(object keyValue) => CreateDbContext().GetAsync<Entity>(keyValue);



        // #3 Update: Update UpdateRange
        public virtual Task<int> UpdateAsync<Entity>(Entity entity) => CreateDbContext().UpdateAsync<Entity>(entity);
        public virtual Task<int> UpdateRangeAsync<Entity>(IEnumerable<Entity> entities) => CreateDbContext().UpdateRangeAsync<Entity>(entities);


        // #4 Delete : Delete DeleteRange DeleteByKey DeleteByKeys
        public virtual Task<int> DeleteAsync<Entity>(Entity entity) => CreateDbContext().DeleteAsync<Entity>(entity);
        public virtual Task<int> DeleteRangeAsync<Entity>(IEnumerable<Entity> entities) => CreateDbContext().DeleteRangeAsync<Entity>(entities);

        public virtual Task<int> DeleteByKeyAsync<Entity>(object keyValue) => CreateDbContext().DeleteByKeyAsync<Entity>(keyValue);
        public virtual Task<int> DeleteByKeysAsync<Entity, Key>(IEnumerable<Key> keys) => CreateDbContext().DeleteByKeysAsync<Entity, Key>(keys);
    }
}
