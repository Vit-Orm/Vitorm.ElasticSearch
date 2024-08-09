using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vitorm.ElasticSearch;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_FilterRule_BatchQuery_Test
    {


        [TestMethod]
        public async Task Test_BatchQuery()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var enumerable = dbContext.BatchQueryAsync<User>(query: null, batchSize: 2);
                List<User> result = new();
                await foreach (var items in enumerable)
                {
                    Assert.AreEqual(2, items.Count);
                    result.AddRange(items);
                }

                Assert.AreEqual(6, result.Count);
            }

            {
                var enumerable = dbContext.BatchQueryAsync<User>(query: null, batchSize: 2).ToEnumerable();
                List<User> result = new();
                foreach (var items in enumerable)
                {
                    Assert.AreEqual(2, items.Count);
                    result.AddRange(items);
                }

                Assert.AreEqual(6, result.Count);
            }

            {
                var enumerable = dbContext.BatchQueryAsync<User>(query: null, batchSize: 2).ToEnumerable().FlattenEnumerable();
                List<User> result = new();
                foreach (var item in enumerable)
                {
                    result.Add(item);
                }
                Assert.AreEqual(6, result.Count);
            }
        }









    }
}
