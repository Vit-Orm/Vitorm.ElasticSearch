using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext : Vitorm.DbContext
    {

        protected static async Task<List<Result>> ToListAsync<Entity, Result>(DbContext self, Expression expression, Dictionary<string, object> queryPayload, Func<Entity, Result> select, string indexName)
        {
            var searchResult = await self.QueryAsync<Entity>(queryPayload, indexName);

            var entityDescriptor = self.GetEntityDescriptor(typeof(Entity));
            var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(self, entityDescriptor));

            if (select == null)
            {
                return entities.ToList() as List<Result>;
            }
            else
            {
                return entities.Select(entity => select(entity)).ToList();
            }
        }



        #region Method cache
        protected static MethodInfo MethodInfo_ToListAsync(Type entityType, Type resultEntityType) =>
            (MethodInfo_ToListAsync_ ??=
                 new Func<DbContext, Expression, Dictionary<string, object>, Func<object, string>, string, Task<List<string>>>(ToListAsync)
                .GetMethodInfo().GetGenericMethodDefinition())
            .MakeGenericMethod(entityType, resultEntityType);


        private static MethodInfo MethodInfo_ToListAsync_;
        #endregion

    }
}
