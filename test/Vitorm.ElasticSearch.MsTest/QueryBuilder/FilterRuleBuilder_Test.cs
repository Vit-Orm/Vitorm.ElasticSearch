using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vit.Core.Module.Serialization;
using Vit.Linq.ComponentModel;
using Vit.Linq.Filter.ComponentModel;

namespace Vitorm.MsTest.QueryBuilder
{

    [TestClass]
    public class FilterRuleBuilder_Test
    {


        [TestMethod]
        public async Task Test()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            var builder = dbContext.filterRuleBuilder;
            builder.AddOperatorMap("Equals", RuleOperator.Equal);

            {
                var strQuery = "{ 'filter':{'field':'children.id',  'operator': '=',  'value': 1 },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(2, result.totalCount);
                Assert.AreEqual(6, result.items[0].id);
            }
            {
                var strQuery = "{ 'filter':{'field':'id',  'operator': 'eQuals',  'value': 1 },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(1, result.totalCount);
                Assert.AreEqual(1, result.items[0].id);
            }

            {
                var strQuery = "{ 'filter':{'field':'id',  'operator': '!=',  'value': 1 },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(5, result.totalCount);
                Assert.AreEqual(6, result.items[0].id);
            }
            {
                var strQuery = "{ 'filter':{'field':'id',  'operator': 'In',  'value': [1,2,3] },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(3, result.totalCount);
                Assert.AreEqual(3, result.items[0].id);
            }
            {
                var strQuery = "{ 'filter':{'field':'id',  'operator': 'NotIn',  'value': [1,2,3] },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(3, result.totalCount);
                Assert.AreEqual(6, result.items[0].id);
            }
            {
                var strQuery = "{ 'filter':{'condition':'not', 'field':'id',  'operator': 'In',  'value': [1,2,3] },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(3, result.totalCount);
                Assert.AreEqual(6, result.items[0].id);
            }
            {
                var strQuery = "{ 'filter':{'condition':'not', 'rules':[{'field':'id',  'operator': 'In',  'value': [1,2,3] }] },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(3, result.totalCount);
                Assert.AreEqual(6, result.items[0].id);
            }

        }


        [TestMethod]
        public async Task Test_IsNull()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            var builder = dbContext.filterRuleBuilder;

            {
                var strQuery = "{ 'filter':{'field':'fatherId',  'operator': '=',  'value': null },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':10, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(3, result.totalCount);
                Assert.AreEqual(6, result.items[0].id);
            }
            {
                var strQuery = "{ 'filter':{'field':'fatherId',  'operator': '!=',  'value': null },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':10, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(3, result.totalCount);
                Assert.AreEqual(3, result.items[0].id);
            }

            {
                var strQuery = "{ 'filter':{'field':'fatherId',  'operator': 'IsNull' },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':10, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(3, result.totalCount);
                Assert.AreEqual(6, result.items[0].id);
            }

            {
                var strQuery = "{ 'filter':{'field':'fatherId',  'operator': 'IsNotNull' },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':10, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(3, result.totalCount);
                Assert.AreEqual(3, result.items[0].id);
            }
        }



        [TestMethod]
        public async Task Test_Sort()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            var builder = dbContext.filterRuleBuilder;

            {
                var strQuery = "{    'orders':[{'field':'name.keyword','asc':false}],  'page':{'pageSize':10, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery<User>(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(6, result.totalCount);
                Assert.AreEqual(6, result.items[0].id);
            }
            {
                var strQuery = "{    'orders':[{'field':'name','asc':false}],  'page':{'pageSize':10, 'pageIndex':1}  }".Replace("'", "\"");
                var query = Json.Deserialize<PagedQuery>(strQuery);

                var queryBody = builder.ConvertToQuery<User>(query.filter);
                var strRequest = Json.Serialize(queryBody);

                var result = await dbContext.QueryAsync<User>(query);
                Assert.AreEqual(6, result.totalCount);
                Assert.AreEqual(6, result.items[0].id);
            }
        }





    }
}
