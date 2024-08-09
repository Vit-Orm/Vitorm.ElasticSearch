using System.Collections.Generic;
using System.Linq;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext
    {

        public class ScrollQueryArgument
        {
            public Dictionary<string, object> queryPayload { get; set; }
            public string indexName { get; set; }
            public int? scrollCacheMinutes { get; set; }
            public bool? useDefaultSort { get; set; }
            public int? maxResultCount { get; set; }

        }

        public virtual IEnumerable<Entity> FlattenBatchQuery<Entity>(ScrollQueryArgument arg) where Entity : class
        {
            return BatchQuery<Entity>(arg).FlattenEnumerable();
        }


        public virtual IEnumerable<List<Entity>> BatchQuery<Entity>(ScrollQueryArgument arg) where Entity : class
        {
            var enumerable = BatchQueryAsync<Entity>(arg);
            return enumerable.ToEnumerable();
        }

        public virtual async IAsyncEnumerable<List<Entity>> BatchQueryAsync<Entity>(ScrollQueryArgument arg) where Entity : class
        {
            // https://www.elastic.co/guide/en/elasticsearch/guide/current/scroll.html

            string scroll_id = null;
            List<Entity> items = null;
            int resultCount = 0;
            int maxResultCount = arg.maxResultCount ?? 10000;
            int scrollCacheMinutes = arg.scrollCacheMinutes ?? 1;
            var entityDescriptor = GetEntityDescriptor(typeof(Entity));

            #region #1 first query
            {
                var queryBody = arg.queryPayload ?? new();
                if (arg.useDefaultSort == true) queryBody["sort"] = new[] { "_doc" };

                queryBody.Remove("track_total_hits");

                var searchUrl = $"{readOnlyServerAddress}/{arg.indexName}/_search?scroll={scrollCacheMinutes}m";

                // send query request
                {
                    var searchResult = await InvokeQueryAsync<QueryResponse<Entity>>(queryBody, searchUrl: searchUrl);
                    scroll_id = searchResult._scroll_id;
                    //items = searchResult?.hits?.hits?.Select(hit => hit?._source).ToList();
                    items = searchResult?.hits?.hits?.Select(hit => hit.GetSource(this, entityDescriptor)).ToList();
                }

                if (items.Any())
                {
                    resultCount += items.Count;
                    if (maxResultCount < resultCount)
                    {
                        var overloadCount = resultCount - maxResultCount;
                        items.RemoveRange(items.Count - overloadCount, overloadCount);
                        resultCount = maxResultCount;
                    }
                    yield return items;
                }

            }
            #endregion

            #region #2 read by scroll
            while (!string.IsNullOrWhiteSpace(scroll_id) && resultCount < maxResultCount)
            {
                var queryBody = new { scroll = scrollCacheMinutes + "m", scroll_id = scroll_id };

                var searchUrl = $"{readOnlyServerAddress}/_search/scroll";

                // send query request
                {
                    var searchResult = await InvokeQueryAsync<QueryResponse<Entity>>(queryBody, searchUrl: searchUrl);
                    scroll_id = searchResult._scroll_id;
                    //items = searchResult?.hits?.hits?.Select(hit => hit?._source).ToList();
                    items = searchResult?.hits?.hits?.Select(hit => hit.GetSource(this, entityDescriptor)).ToList();
                }

                if (items?.Any() != true)
                {
                    break;
                }

                resultCount += items.Count;
                if (maxResultCount < resultCount)
                {
                    var overloadCount = resultCount - maxResultCount;
                    items.RemoveRange(items.Count - overloadCount, overloadCount);
                    resultCount = maxResultCount;
                }
                yield return items;
            }
            #endregion

        }



    }
}
