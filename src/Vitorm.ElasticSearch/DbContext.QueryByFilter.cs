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


        public virtual async Task<RangeData<Entity>> QueryAsync<Entity>(RangedQuery query, string indexName = null)
        {
            var queryPayload = filterRuleBuilder.ConvertToQueryPayload<Entity>(query, maxResultWindowSize: maxResultWindowSize, track_total_hits: track_total_hits);

            indexName ??= GetIndex<Entity>();

            var searchResult = await QueryAsync<Entity>(queryPayload, indexName);

            var entityDescriptor = GetEntityDescriptor(typeof(Entity));
            var items = searchResult?.hits?.hits?.Select(hit => hit.GetSource(this, entityDescriptor)).ToList();

            var totalCount = searchResult?.hits?.total?.value ?? 0;
            return new RangeData<Entity>(query.range) { items = items, totalCount = totalCount };
        }

        public virtual IAsyncEnumerable<List<Entity>> BatchQueryAsync<Entity>(
            RangedQuery query, string indexName = null,
            int batchSize = 5000, int scrollCacheMinutes = 1, bool useDefaultSort = false
        ) where Entity : class
        {
            int skip = query?.range?.skip ?? 0;
            int take = batchSize;
            int maxResultCount = query?.range?.take ?? 0;
            if (maxResultCount <= 0) maxResultCount = int.MaxValue;

            query ??= new();
            query.range = new(skip: skip, take: take);
            var queryPayload = filterRuleBuilder.ConvertToQueryPayload<Entity>(query);

            indexName ??= GetIndex<Entity>();

            var arg = new ScrollQueryArgument
            {
                queryPayload = queryPayload,
                indexName = indexName,
                scrollCacheMinutes = scrollCacheMinutes,
                useDefaultSort = useDefaultSort,
                maxResultCount = maxResultCount
            };

            return BatchQueryAsync<Entity>(arg);
        }


    }
}
