using System.Data;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vit.Extensions.Serialize_Extensions;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_Type_DateTime_Test
    {

        [TestMethod]
        public void Test_Equal()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            // ==
            {
                var query = userQuery.Where(u => u.birth == new DateTime(2021, 01, 01, 03, 00, 00));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(1, userList.Count);
                Assert.AreEqual(3, userList.First().id);
            }
        }

        [TestMethod]
        public void Test_Compare()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var query = userQuery.Where(u => u.birth >= new DateTime(2021, 01, 01, 05, 00, 00));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(2, userList.Count);
                Assert.AreEqual(0, userList.Select(m => m.id).Except(new[] { 5, 6 }).Count());
            }
            {
                var query = userQuery.Where(u => u.strBirth.Convert<DateTime>() >= new DateTime(2021, 01, 01, 05, 00, 00));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(2, userList.Count);
                Assert.AreEqual(0, userList.Select(m => m.id).Except(new[] { 5, 6 }).Count());
            }
        }



        [TestMethod]
        public void Test_Calculate()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var query = userQuery.Where(u => u.birth >= DateTime.Parse("2021-01-01 01:00:00").AddHours(4));
                var strQuery = query.ToExecuteString();
                var userList = query.ToList();
                Assert.AreEqual(2, userList.Count);
                Assert.AreEqual(0, userList.Select(m => m.id).Except(new[] { 5, 6 }).Count());
            }

        }



    }
}
