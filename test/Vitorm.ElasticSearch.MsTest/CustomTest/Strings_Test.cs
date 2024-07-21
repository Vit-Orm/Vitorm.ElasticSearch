using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vitorm.ElasticSearch;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Strings_Test
    {


        [TestMethod]
        public void Test_Query()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            // #1 ==
            {
                var query = userQuery.Where(user => user.name == "u146");
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
                Assert.AreEqual(1, userList[0].id);
            }

            // #2 !=
            {
                var query = userQuery.Where(user => user.name != "u146");
                var userList = query.ToList();
                Assert.AreEqual(5, userList.Count);
            }


            // #3 Like
            {
                var query = userQuery.Where(user => user.name.StartsWith("u1"));
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
            }
            {
                var query = userQuery.Where(user => user.name.EndsWith("46"));
                var userList = query.ToList();
                Assert.AreEqual(2, userList.Count);
            }
            {
                var query = userQuery.Where(user => user.name.Contains("24"));
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
            }
            {
                var query = userQuery.Where(user => user.name.Contains("246"));
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
            }
            {
                var query = userQuery.Where(user => user.name.Like("*246"));
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
            }


            // #4 Match
            {
                var query = userQuery.Where(user => user.remarks.Match("u400"));
                var userList = query.ToList();
                Assert.AreEqual(3, userList.Count);
            }
            {
                var query = userQuery.Where(user => user.remarks.Match("u40"));
                var userList = query.ToList();
                Assert.AreEqual(0, userList.Count);
            }

        }



    }
}
