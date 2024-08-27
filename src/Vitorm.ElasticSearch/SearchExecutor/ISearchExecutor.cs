using System.Threading.Tasks;

using Vitorm.ElasticSearch.QueryExecutor;

namespace Vitorm.ElasticSearch.SearchExecutor
{
    public interface ISearchExecutor
    {
        Task<bool> ExecuteSearchAsync<Entity, ResultEntity>(SearchExecutorArgument<ResultEntity> arg);
    }
}
