
# Vitorm.ElasticSearch.QueryBuilder
Tool to convert FilterRule or ExpressionNode to ElasticSearch Query Request
>source address: [https://github.com/VitormLib/Vitorm.ElasticSearch.QueryBuilder](https://github.com/VitormLib/Vitorm.ElasticSearch.QueryBuilder)    

![](https://img.shields.io/github/license/VitormLib/Vitorm.ElasticSearch.svg)  
![](https://img.shields.io/github/repo-size/VitormLib/Vitorm.ElasticSearch.svg)  ![](https://img.shields.io/github/last-commit/VitormLib/Vitorm.ElasticSearch.svg)  
 

| Build | NuGet |
| -------- | -------- |
|![](https://github.com/VitormLib/Vitorm.ElasticSearch/workflows/ki_devops3/badge.svg) | [![](https://img.shields.io/nuget/v/Vitorm.ElasticSearch.svg)](https://www.nuget.org/packages/Vitorm.ElasticSearch.QueryBuilder) ![](https://img.shields.io/nuget/dt/Vitorm.ElasticSearch.QueryBuilder.svg) |




## Installation
Before using , install the necessary package:    
``` bash
dotnet add package Vitorm.ElasticSearch.QueryBuilder
dotnet add package Vit.Core
```

## Demo
``` csharp
using Vit.Core.Module.Serialization;
using Vit.Linq.ComponentModel;
using Vitorm.ElasticSearch;

namespace App
{
    public class Program
    {
        static void Main(string[] args)
        {
            var strPagedQuery = "{ 'filter':{'field':'children.id',  'operator': '=',  'value': 1 },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
            var pagedQuery = Json.Deserialize<PagedQuery>(strPagedQuery);


            var queryRequest = new FilterRuleBuilder().ConvertToQuery(pagedQuery.filter);
            var strQuery = Json.Serialize(queryRequest);
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

            public List<User> children { get; set; }
        }
    }
}
```
