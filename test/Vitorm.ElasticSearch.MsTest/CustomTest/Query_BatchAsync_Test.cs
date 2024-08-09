using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vit.Linq;

using Vitorm.ElasticSearch;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_BatchAsync_Test
    {

        [TestMethod]
        public async Task Test_BatchAsync()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var enumerable = userQuery.Where(user => user.id > 2).BatchAsync(batchSize: 2);

                List<User> result = new();
                await foreach (var items in enumerable)
                {
                    Assert.AreEqual(2, items.Count);
                    result.AddRange(items);
                }
                Assert.AreEqual(4, result.Count);
            }

            {
                var enumerable = userQuery.Where(user => user.id > 2).BatchAsync(batchSize: 2, scrollCacheMinutes: 2, useDefaultSort: true);

                List<User> result = new();
                await foreach (var items in enumerable)
                {
                    Assert.AreEqual(2, items.Count);
                    result.AddRange(items);
                }
                Assert.AreEqual(4, result.Count);
            }

            {
                var enumerable = userQuery.Where(user => user.id > 2).ToEnumerableByBatch(batchSize: 2);
                List<User> result = new();
                foreach (var item in enumerable)
                {
                    result.Add(item);
                }
                Assert.AreEqual(4, result.Count);
            }

            {
                var enumerable = userQuery.Where(user => user.id > 2).BatchAsync(batchSize: 2).ToEnumerable();
                List<User> result = new();
                foreach (var items in enumerable)
                {
                    Assert.AreEqual(2, items.Count);
                    result.AddRange(items);
                }

                Assert.AreEqual(4, result.Count);
            }

            {
                var enumerable = userQuery.Where(user => user.id > 2).BatchAsync(batchSize: 2).ToEnumerable().FlattenEnumerable();
                List<User> result = new();
                foreach (var item in enumerable)
                {
                    result.Add(item);
                }
                Assert.AreEqual(4, result.Count);
            }



        }



    }
}
