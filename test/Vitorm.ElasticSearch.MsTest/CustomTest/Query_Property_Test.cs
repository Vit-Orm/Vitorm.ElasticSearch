using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vitorm.ElasticSearch;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_Property_Test
    {


        [TestMethod]
        public void Test_Property()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var query = userQuery.Where(user => user.Property<string>("strId") == "2");
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
                Assert.AreEqual(2, userList[0].id);
            }
            {
                var query = userQuery.Where(user => user.Property<int>("strId") == 2);
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
                Assert.AreEqual(2, userList[0].id);
            }
            {
                var query = userQuery.Where(user => user.Property<int>("father.id") == 5);
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
                Assert.AreEqual(3, userList[0].id);
            }



        }



    }
}
