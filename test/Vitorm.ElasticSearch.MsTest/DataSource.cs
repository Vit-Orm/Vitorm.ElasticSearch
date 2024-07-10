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

        public int id { get; set; }
        public string name { get; set; }
        public DateTime? birth { get; set; }

        public int? fatherId { get; set; }
        public int? motherId { get; set; }


        public User father { get; set; }
        public User mother { get; set; }

        public static User NewUser(int id, bool forAdd = false) => new User { key = id.ToString(), id = id, name = "testUser" + id };

        public static List<User> NewUsers(int startId, int count = 1, bool forAdd = false)
        {
            return Enumerable.Range(startId, count).Select(id => NewUser(id, forAdd)).ToList();
        }

    }


    public class DataSource
    {
        public static void WaitForUpdate() => Thread.Sleep(2000);

        static string connectionString = Appsettings.json.GetStringByPath("Vitorm.ElasticSearch.connectionString");


        static int dbIndexCount = 0;
        public static Vitorm.ElasticSearch.DbContext CreateDbContextForWriting()
        {
            var dbContext = new Vitorm.ElasticSearch.DbContext(connectionString);

            var dbSet = dbContext.DbSet<User>();

            dbSet.ChangeTable(dbSet.entityDescriptor.tableName + "2");

            InitDbContext(dbContext);
            return dbContext;
        }


        static bool initedDefaultIndex = false;
        public static Vitorm.ElasticSearch.DbContext CreateDbContext()
        {
            var dbContext = new Vitorm.ElasticSearch.DbContext(connectionString);

            lock (typeof(DataSource))
            {
                if (!initedDefaultIndex)
                {
                    InitDbContext(dbContext);
                    initedDefaultIndex = true;
                }
            }
            return dbContext;
        }

        static void InitDbContext(Vitorm.ElasticSearch.DbContext dbContext)
        {
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


            dbContext.Drop<User>();
            dbContext.Create<User>();
            dbContext.AddRange(users);

            WaitForUpdate();
        }
    }
}
