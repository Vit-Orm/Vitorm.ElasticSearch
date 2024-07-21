using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_Object_Test
    {


        [TestMethod]
        public void Test_Query()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var query = userQuery.Where(user => user.father.id == 4);
                var queryBody = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(2, userList.Count);
                Assert.AreEqual(0, userList.Select(m => m.id).Except(new[] { 1, 2 }).Count());
            }



        }



    }
}
