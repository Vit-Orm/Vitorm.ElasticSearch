using Vit.Core.Util.ConfigurationManager;

namespace Vitorm.MsTest
{
    [System.ComponentModel.DataAnnotations.Schema.Table("User")]
    public class User
    {
        [System.ComponentModel.DataAnnotations.Key]
        [System.Text.Json.Serialization.JsonIgnore]
        [Newtonsoft.Json.JsonIgnore]
        public string key { get; set; }

 
        //[System.Text.Json.Serialization.JsonIgnore]
        //[Newtonsoft.Json.JsonIgnore]
        public int id { get; set; }
        //{
        //    get => int.TryParse(key, out var v) ? v : 0;
        //    set => key = value.ToString();
        //}
        public string name { get; set; }
        public DateTime? birth { get; set; }

        public int? fatherId { get; set; }
        public int? motherId { get; set; }


        public User father { get; set; }
        public User mother { get; set; }

    }


    public class DataSource
    {
        static string connectionString = Appsettings.json.GetStringByPath("App.Db.ConnectionString");

        static int dbIndexCount = 0;
        static bool initedDefaultIndex = false;
        public static Vitorm.ElasticSearch.DbContext CreateDbContext()
        {
       
            var dbContext = new Vitorm.ElasticSearch.DbContext(connectionString);

            //dbIndexCount++;
            //var dbIndexName = "dev-orm-" + dbIndexCount;
            //dbContext.GetEntityIndex=(_)=> dbIndexName;

            var users = new List<User> {
                    new User { key="1",id=1, name="u146", fatherId=4, motherId=6 },
                    new User { key="2",id=2, name="u246", fatherId=4, motherId=6 },
                    new User { key="3",id=3, name="u356", fatherId=5, motherId=6 },
                    new User { key="4",id=4, name="u400" },
                    new User { key="5",id=5, name="u500" },
                    new User { key="6",id=6, name="u600" },
                };

            users.ForEach(user => { user.birth = DateTime.Parse("2021-01-01 00:00:00").AddHours(user.id); });

            users.ForEach(user =>
            {
                user.father = users.FirstOrDefault(m => m.id == user.fatherId);
                user.mother = users.FirstOrDefault(m => m.id == user.motherId);
            });

            lock (typeof(DataSource))
            {
                //if (!initedDefaultIndex)
                //{
                //    dbContext.Drop<User>();
                //    dbContext.Create<User>();
                //    dbContext.AddRange(users);

                //    Thread.Sleep(2000);
                //    initedDefaultIndex = true;
                //}
            }
      
            return dbContext;
        }

    }
}
