using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Vitorm.ElasticSearch.QueryExecutor;
using Vitorm.StreamQuery;

using static Vitorm.ElasticSearch.DbContext;

namespace Vitorm.ElasticSearch.SearchExecutor
{
    public class PlainSearchExecutor : ISearchExecutor
    {
        public async Task<bool> ExecuteSearchAsync<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg)
        {
            var combinedStream = arg.combinedStream;
            var dbContext = arg.dbContext;

            if (combinedStream.source is not SourceStream) return false;
            if (combinedStream.isGroupedStream) return false;
            if (combinedStream.joins?.Any() == true) return false;
            if (combinedStream.distinct != null) return false;


            var queryPayload = dbContext.ConvertStreamToQueryPayload(combinedStream);

            var searchResult = await dbContext.ExecuteSearchAsync<QueryResponse<Entity>>(queryPayload, indexName: arg.indexName);


            #region getList
            if (arg.getList)
            {
                var entityDescriptor = dbContext.GetEntityDescriptor(typeof(Entity));
                var entities = searchResult?.hits?.hits?.Select(hit => hit.GetSource(dbContext, entityDescriptor));
                Func<Entity, ResultEntity> funcSelect = DbContext.BuildSelect<Entity, ResultEntity>(combinedStream, dbContext.convertService);

                if (funcSelect == null)
                {
                    arg.list = entities as IEnumerable<ResultEntity>;
                }
                else
                {
                    arg.list = entities.Select(entity => funcSelect(entity));
                }
            }
            #endregion


            arg.totalCount = searchResult?.hits?.total?.value;

            return true;
        }
    }
}
