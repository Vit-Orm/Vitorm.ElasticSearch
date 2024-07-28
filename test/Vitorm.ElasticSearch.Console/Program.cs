using System;
using System.Linq;
using Vitorm;
namespace App
{
    public class Program
    {
        static void Main(string[] args)
        {
            // #1 Configures Vitorm
            using var dbContext = new Vitorm.ElasticSearch.DbContext("http://localhost:9200");

            // #2 Create Table
            dbContext.TryDropTable<User>();
            dbContext.TryCreateTable<User>();

            // #3 Insert Records
            dbContext.Add(new User { id = 1, name = "lith" });
            dbContext.AddRange(new[] {
                new User {   id = 2, name = "lith", fatherId = 1 },
                new User {   id = 3, name = "lith", fatherId = 1 }
            });

            // #4 Query Records
            {
                var user = dbContext.Get<User>(1);
                var users = dbContext.Query<User>().Where(u => u.name.Contains("li")).ToList();
                var sql = dbContext.Query<User>().Where(u => u.name.Contains("li")).ToExecuteString();
            }

            // #5 Update Records
            dbContext.Update(new User { id = 1, name = "lith1" });
            dbContext.UpdateRange(new[] {
                new User {   id = 2, name = "lith2", fatherId = 1 },
                new User {   id = 3, name = "lith3", fatherId = 2 }
            });
            //dbContext.Query<User>().Where(u => u.name.Contains("li"))
            //    .ExecuteUpdate(u => new User { name = "Lith" + u.id });

            // #6 Delete Records
            dbContext.Delete<User>(new User { id = 1 });
            dbContext.DeleteRange(new[] {
                new User {  id = 2 },
                new User {  id = 3 }
            });
            dbContext.DeleteByKey<User>(1);
            dbContext.DeleteByKeys<User, int>(new[] { 1, 2 });
            //dbContext.Query<User>().Where(u => u.name.Contains("li"))
            //    .ExecuteDelete();

            // #7 Join Queries

            // #8 Transactions

            // #9 Database Functions
        }

        // Entity Definition
        [System.ComponentModel.DataAnnotations.Schema.Table("User")]
        public class User
        {
            [System.ComponentModel.DataAnnotations.Key]
            public int id { get; set; }
            public string name { get; set; }
            public DateTime? birth { get; set; }
            public int? fatherId { get; set; }
        }
    }
}