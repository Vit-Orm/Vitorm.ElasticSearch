using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vitorm.ElasticSearch;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_Other_Test
    {


        [TestMethod]
        public void Test_Not()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var query = userQuery.Where(user => user.id == 2);
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
                Assert.AreEqual(2, userList[0].id);
            }
            {
                var query = userQuery.Where(user => user.id != 2);
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(5, userList.Count);
            }
            {
                var query = userQuery.Where(user => !(user.id == 2));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(5, userList.Count);
            }
            {
                var query = userQuery.Where(user => !(user.id != 2));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
            }
            {
                var query = userQuery.Where(user => user.id < 2);
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
            }
            {
                var query = userQuery.Where(user => !(user.id < 2));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(5, userList.Count);
            }
            {
                var query = userQuery.Where(user => !(new[] { 1, 2 }.Contains(user.id)));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(4, userList.Count);
            }

        }


        [TestMethod]
        public void Test_IsNull()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var query = userQuery.Where(user => user.father == null);
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(3, userList.Count);
                Assert.AreEqual(0, userList.Select(m => m.id).Except(new[] { 4, 5, 6 }).Count());
            }
            {
                var query = userQuery.Where(user => user.father != null);
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(3, userList.Count);
                Assert.AreEqual(0, userList.Select(m => m.id).Except(new[] { 1, 2, 3 }).Count());
            }
            {
                var query = userQuery.Where(user => !(user.father == null));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(3, userList.Count);
                Assert.AreEqual(0, userList.Select(m => m.id).Except(new[] { 1, 2, 3 }).Count());
            }
            {
                var query = userQuery.Where(user => !(user.father != null));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(3, userList.Count);
                Assert.AreEqual(0, userList.Select(m => m.id).Except(new[] { 4, 5, 6 }).Count());
            }


        }



    }
}
