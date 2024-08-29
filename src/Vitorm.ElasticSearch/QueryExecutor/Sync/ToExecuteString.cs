namespace Vitorm.ElasticSearch.QueryExecutor
{
    public partial class ToExecuteString : IQueryExecutor
    {
        public static readonly ToExecuteString Instance = new();

        public string methodName => nameof(Orm_Extensions.ToExecuteString);

        public object ExecuteQuery(QueryExecutorArgument execArg)
        {
            var queryPayload = execArg.dbContext.ConvertStreamToQueryPayload(execArg.combinedStream);

            var url = $"{execArg.dbContext.readOnlyServerAddress}/{execArg.indexName}/_search";
            execArg.dbContext.Event_OnExecuting(executeString: url, param: queryPayload);

            return execArg.dbContext.Serialize(queryPayload);
        }
    }
}
