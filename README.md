
# Vitorm.ElasticSearch
Vitorm.ElasticSearch: an simple orm for ElasticSearch
>source address: [https://github.com/Vit-Orm/Vitorm.ElasticSearch](https://github.com/Vit-Orm/Vitorm.ElasticSearch "https://github.com/Vit-Orm/Vitorm.ElasticSearch")    

![](https://img.shields.io/github/license/Vit-Orm/Vitorm.ElasticSearch.svg)  
![](https://img.shields.io/github/repo-size/Vit-Orm/Vitorm.ElasticSearch.svg)  ![](https://img.shields.io/github/last-commit/Vit-Orm/Vitorm.ElasticSearch.svg)  
 

| Build | NuGet |
| -------- | -------- |
|![](https://github.com/Vit-Orm/Vitorm.ElasticSearch/workflows/ki_devops3/badge.svg) | [![](https://img.shields.io/nuget/v/Vitorm.ElasticSearch.svg)](https://www.nuget.org/packages/Vitorm.ElasticSearch) ![](https://img.shields.io/nuget/dt/Vitorm.ElasticSearch.svg) |




# Vitorm.ElasticSearch Documentation
This guide will walk you through the steps to set up and use Vitorm.ElasticSearch.

supported features:

| feature    |  method   |  remarks   |     |
| --- | --- | --- | --- |
|  create table   |  TryCreateTable   |     |     |
|  drop table   |  TryDropTable   |     |     |
| --- | --- | --- | --- |
|  create records   |  Add AddRange   |     |     |
|  retrieve  records |  Query Get   |     |     |
|  update records   |  Update UpdateRange ExecuteUpdate  |     |     |
|  delete records   |  Delete DeleteRange DeleteByKey DeleteByKeys ExecuteDelete   |     |     |
| --- | --- | --- | --- |
|  change table   |  ChangeTable    |  change mapping table from database   |   |
|  change database  |  ChangeDatabase   | change database to be connected  |   |
| --- | --- | --- | --- |
|  collection total count   |  TotalCount    |  Collection Total Count without Take and Skip   |   |
|  collection total count and list  |  ToListAndTotalCount   | query List and TotalCount at on request  |   |
|     |     |   |   |


## Installation
Before using Vitorm, install the necessary package:    
``` bash
dotnet add package Vitorm.ElasticSearch
```

## Minimum viable demo
> code address: [Program_Min.cs](https://github.com/Vit-Orm/Vitorm.ElasticSearch/tree/master/test/Vitorm.ElasticSearch.Console/Program_Min.cs)    
``` csharp
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
```


## Full Example
> This example provides a comprehensive guide to utilizing Vitorm for basic and advanced database operations while maintaining lightweight performance.    
> code address: [Program.cs](https://github.com/Vit-Orm/Vitorm.ElasticSearch/tree/master/test/Vitorm.ElasticSearch.Console/Program.cs)    
``` csharp
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
```

## Explanation   
1. **Setup**: Initializes the database and configures Vitorm.
2. **Create Table**: Drops and recreates the `User` table.
3. **Insert Records**: Adds single and multiple user records.
4. **Query Records**: Retrieves user records using various querying methods.
5. **Update Records**: Updates single and multiple user records.
6. **Delete Records**: Deletes single and multiple user records.
7. **Join Queries**: Performs a join operation between user and father records.
8. **Transactions**: Demonstrates nested transactions and rollback/commit operations.
9. **Database Functions**: Uses custom database functions in queries.



# Vitorm.Data Documentation    
Vitorm.Data is a static class that allows you to use Vitorm without explicitly creating or disposing of a DbContext.    
 
## Installation    
Before using Vitorm.Data, install the necessary package:    
``` bash
dotnet add package Vitorm.Data
dotnet add package Vitorm.ElasticSearch
```

## Config settings
``` json
// appsettings.json
{
  "Vitorm": {
    "Data": [
      {
        "provider": "ElasticSearch",
        "namespace": "App",
        "connectionString": "http://localhost:9200"
      }
    ]
  }
}
```

## Minimum viable demo
> After configuring the `appsettings.json` file, you can directly perform queries without any additional configuration or initialization, `Vitorm.Data` is that easy to use.    
> code address: [Program_Min.cs](https://github.com/Vit-Orm/Vitorm/tree/master/test/Vitorm.Data.Console/Program_Min.cs)    
``` csharp
using Vitorm;
namespace App
{
    public class Program_Min
    {
        static void Main2(string[] args)
        {
            //  Query Records
            var user = Data.Get<User>(1);
            var users = Data.Query<User>().Where(u => u.name.Contains("li")).ToList();
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

```

## Full Example    
> code address: [Program.cs](https://github.com/Vit-Orm/Vitorm/tree/master/test/Vitorm.Data.Console/Program.cs)    
``` csharp
using Vitorm;

namespace App
{
    public class Program
    {
        static void Main(string[] args)
        {
            // #1 No need to init Vitorm.Data

            // #2 Create Table
            Data.TryDropTable<User>();
            Data.TryCreateTable<User>();

            // #3 Insert Records
            Data.Add(new User { id = 1, name = "lith" });
            Data.AddRange(new[] {
                new User { id = 2, name = "lith", fatherId = 1 },
                new User { id = 3, name = "lith", fatherId = 1 }
            });

            // #4 Query Records
            {
                var user = Data.Get<User>(1);
                var users = Data.Query<User>().Where(u => u.name.Contains("li")).ToList();
                var sql = Data.Query<User>().Where(u => u.name.Contains("li")).ToExecuteString();
            }

            // #5 Update Records
            Data.Update(new User { id = 1, name = "lith1" });
            Data.UpdateRange(new[] {
                new User { id = 2, name = "lith2", fatherId = 1 },
                new User { id = 3, name = "lith3", fatherId = 2 }
            });
            Data.Query<User>().Where(u => u.name.Contains("li"))
                .ExecuteUpdate(u => new User { name = "Lith" + u.id });

            // #6 Delete Records
            Data.Delete<User>(new User { id = 1, name = "lith1" });
            Data.DeleteRange(new[] {
                new User { id = 2, name = "lith2", fatherId = 1 },
                new User { id = 3, name = "lith3", fatherId = 2 }
            });
            Data.DeleteByKey<User>(1);
            Data.DeleteByKeys<User, int>(new[] { 1, 2 });
            Data.Query<User>().Where(u => u.name.Contains("li"))
                .ExecuteDelete();

            // #7 Join Queries

            // #8 Transactions

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
```


# Examples:  
[Test Example](https://github.com/Vit-Orm/Vitorm.ElasticSearch/tree/master/test/Vitorm.ElasticSearch.MsTest)    



