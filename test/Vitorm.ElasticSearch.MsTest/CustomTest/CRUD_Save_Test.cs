using Microsoft.VisualStudio.TestTools.UnitTesting;

using System.Data;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class CRUD_Save_Test
    {
        static Vitorm.ElasticSearch.DbContext CreateDbContext() => DataSource.CreateDbContextForWriting();




        [TestMethod]
        public void Test_Save()
        {
            using var dbContext = CreateDbContext();

            // Save
            {
                var user = User.NewUsers(5)[0];
                var rowCount = dbContext.Save(user);
                Assert.AreEqual(1, rowCount);
            }
            {
                var user = User.NewUsers(8)[0];
                user.key = null;
                var rowCount = dbContext.Save(user);
                Assert.AreEqual(1, rowCount);
            }

            // SaveRange
            {
                var users = User.NewUsers(6, 2);
                dbContext.SaveRange(users);
            }

            Thread.Sleep(1000);

            // assert
            {
                var newUserList = User.NewUsers(5, 4);
                var userList = dbContext.Query<User>().Where(m => m.id >= 5).ToList();
                Assert.AreEqual(newUserList.Count, userList.Count());
                Assert.AreEqual(0, userList.Select(m => m.id).Except(newUserList.Select(m => m.id)).Count());
                Assert.AreEqual(0, userList.Select(m => m.name).Except(newUserList.Select(m => m.name)).Count());
            }


        }



    }
}
