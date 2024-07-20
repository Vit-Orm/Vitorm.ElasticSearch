using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vit.Core.Module.Serialization;
using Vit.Linq.ComponentModel;

using Vitorm.ElasticSearch;

namespace Vitorm.MsTest.QueryBuilder
{

    [TestClass]
    public class QueryBuilder_Test
    {


        [TestMethod]
        public async Task Test()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();
            {
                var strPagedQuery = "{ 'filter':{'field':'children.id',  'operator': '=',  'value': 1 },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
                var pagedQuery = Json.Deserialize<PagedQuery>(strPagedQuery);


                var queryRequest = new FilterRuleBuilder().ConvertToQuery(pagedQuery.filter);
                var strQuery = Json.Serialize(queryRequest);

                var result = await dbContext.QueryAsync<User>(pagedQuery.ToRangedQuery());

                Assert.AreEqual(2, result.totalCount);
                Assert.AreEqual(6, result.items[0].id);
            }

        }








    }
}
