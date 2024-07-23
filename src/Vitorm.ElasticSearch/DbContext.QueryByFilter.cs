using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Vit.Linq.ComponentModel;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext
    {
        private static FilterRuleBuilder defaultFilterRuleBuilder_;
        public static FilterRuleBuilder defaultFilterRuleBuilder
        {
            get => defaultFilterRuleBuilder_ ?? (defaultFilterRuleBuilder_ = new());
            set => defaultFilterRuleBuilder_ = value;
        }

        public FilterRuleBuilder filterRuleBuilder = defaultFilterRuleBuilder;

        public virtual async Task<PageData<Entity>> QueryAsync<Entity>(PagedQuery query)
        {
            var data = await QueryAsync<Entity>(query.ToRangedQuery());

            return new(query.page) { totalCount = data.totalCount, items = data.items };
        }

        public virtual async Task<RangeData<Entity>> QueryAsync<Entity>(RangedQuery query)
        {
            var queryBody = new Dictionary<string, object>();

            #region queryBody
            {
                // #1 where
                queryBody["query"] = filterRuleBuilder.ConvertToQuery(query.filter);

                // #2 orders
                if (query.orders?.Any() == true)
                {
                    queryBody["sort"] = query.orders
                                     .Select(order => new Dictionary<string, object> { [order.field] = new { order = order.asc ? "asc" : "desc" } })
                                     .ToList();
                }

                // #3 skip take
                if (query.range?.skip > 0)
                    queryBody["from"] = query.range.skip;
                if (query.range?.take > 0)
                    queryBody["size"] = query.range.take;
            }
            #endregion


            var entityDescriptor = GetEntityDescriptor(typeof(Entity));
            var indexName = GetIndex<Entity>();

            var searchResult = await QueryAsync<Entity>(queryBody, indexName);
            var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(entityDescriptor));

            var items = entities.ToList();
            var totalCount = searchResult?.hits?.total?.value ?? 0;
            return new RangeData<Entity>(query.range) { items = items, totalCount = totalCount };

        }



    }
}
