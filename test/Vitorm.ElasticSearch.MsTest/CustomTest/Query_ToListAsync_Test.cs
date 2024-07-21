using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vit.Linq;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_ToListAsync_Test
    {

        [TestMethod]
        public async Task Test_Query()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var query = userQuery.Where(user => user.father.id == 4);

                var userList = await query.ToListAsync();
                Assert.AreEqual(2, userList.Count);
                Assert.AreEqual(0, userList.Select(m => m.id).Except(new[] { 1, 2 }).Count());
            }



        }



    }
}
