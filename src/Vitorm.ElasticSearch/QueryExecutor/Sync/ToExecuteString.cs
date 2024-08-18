namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class ToExecuteString : IQueryExecutor
    {
        public static readonly ToExecuteString Instance = new();

        public string methodName => nameof(Orm_Extensions.ToExecuteString);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var queryPayload = execArg.dbContext.ConvertStreamToQueryPayload(execArg.combinedStream);
            return execArg.dbContext.Serialize(queryPayload);
        }
    }
}
