using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Refresh_Test
    {

        [TestMethod]
        public void Test_Refresh()
        {
            using var dbContext = DataSource.CreateDbContextForWriting();
            dbContext.refresh = null;

            var userQuery = dbContext.Query<User>();
            var users = new List<User> {
                User.NewUser(id: 7, forAdd: true),
                User.NewUser(id: 8, forAdd: true),
            };
            dbContext.AddRange(users);

            // assert
            {
                var userList = dbContext.Query<User>().ToList();
                //Assert.AreEqual(6, userList.Count()); 
            }


            dbContext.Refresh<User>();
            // assert
            {
                var userList = dbContext.Query<User>().ToList();
                Assert.AreEqual(8, userList.Count());
            }

        }



    }
}
