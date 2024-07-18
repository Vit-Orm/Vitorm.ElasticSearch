using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vitorm.ElasticSearch;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_Array_Test
    {


        [TestMethod]
        public void Test_Query()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var query = userQuery.Where(user => user.children.Who().id == 1);
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(2, userList.Count);
            }



        }



    }
}
