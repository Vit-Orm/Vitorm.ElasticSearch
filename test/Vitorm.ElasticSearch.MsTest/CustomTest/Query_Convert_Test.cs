using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vitorm.ElasticSearch;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_Convert_Test
    {


        [TestMethod]
        public void Test_Convert()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var query = userQuery.Where(user => user.strId == "2");
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
                Assert.AreEqual(2, userList[0].id);
            }
            {
                var query = userQuery.Where(user => user.strId.Convert<int>() == 2);
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
                Assert.AreEqual(2, userList[0].id);
            }
            {
                var query = userQuery.Where(user => new[] { 1, 2 }.Contains(user.strId.Convert<int>()));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(2, userList.Count);
            }



        }



    }
}
