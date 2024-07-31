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
            var data = await QueryAsync<Entity>(query);

            return new(query.page) { totalCount = data.totalCount, items = data.items };
        }


        public virtual async Task<RangeData<Entity>> QueryAsync<Entity>(RangedQuery query)
        {
            var queryPayload = filterRuleBuilder.ConvertToQueryPayload(query, maxResultWindowSize: maxResultWindowSize, track_total_hits: track_total_hits);

            var entityDescriptor = GetEntityDescriptor(typeof(Entity));
            var indexName = GetIndex<Entity>();

            var searchResult = await QueryAsync<Entity>(queryPayload, indexName);
            var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(this, entityDescriptor));

            var items = entities.ToList();
            var totalCount = searchResult?.hits?.total?.value ?? 0;
            return new RangeData<Entity>(query.range) { items = items, totalCount = totalCount };

        }



    }
}
