using System;
using System.Linq;
using System.Threading;
namespace App
{
    public class Program_Min
    {
        static void Main2(string[] args)
        {
            // #1 Init
            using var dbContext = new Vitorm.ElasticSearch.DbContext("http://localhost:9200");
            dbContext.TryDropTable<User>();
            dbContext.TryCreateTable<User>();
            dbContext.Add(new User { id = 1, name = "lith" });
            Thread.Sleep(2000);

            // #2 Query
            var user = dbContext.Get<User>(1);
            var users = dbContext.Query<User>().Where(u => u.name.Contains("li")).ToList();
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