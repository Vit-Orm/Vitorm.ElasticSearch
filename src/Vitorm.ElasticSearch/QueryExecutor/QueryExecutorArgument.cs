using System;
using System.Linq.Expressions;

using Vitorm.StreamQuery;

namespace Vitorm.ElasticSearch.QueryExecutor
{
    public class QueryExecutorArgument
    {
        public CombinedStream combinedStream;
        public Vitorm.ElasticSearch.DbContext dbContext;
        public string indexName;

        public Type entityType;
        public Expression expression;
    }

}
