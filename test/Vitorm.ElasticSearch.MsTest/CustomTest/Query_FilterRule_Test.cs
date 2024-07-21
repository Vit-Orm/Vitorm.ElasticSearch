using System.Collections;
using System.Linq.Expressions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Vit.Core.Module.Serialization;
using Vit.Linq;
using Vit.Linq.ComponentModel;
using Vit.Linq.Filter;
using Vit.Linq.Filter.ComponentModel;

using Vitorm.ElasticSearch;

namespace Vitorm.MsTest.CustomTest
{

    [TestClass]
    public class Query_FilterRule_Test
    {


        [TestMethod]
        public void Test()
        {
            using var dbContext = DataSource.CreateDbContext();
            var userQuery = dbContext.Query<User>();

            {
                var strPagedQuery = "{ 'filter':{'field':'father.name',  'operator': '=',  'value': 'u400' },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
                var pagedQuery = Json.Deserialize<PagedQuery>(strPagedQuery);
                var pageData = userQuery.ToPageData(pagedQuery);

                Assert.AreEqual(2, pageData.totalCount);
                Assert.AreEqual(2, pageData.items[0].id);
            }
            {
                var strPagedQuery = "{ 'filter':{'field':'children.id',  'operator': '=',  'value': 1 },  'orders':[{'field':'id','asc':false}],  'page':{'pageSize':2, 'pageIndex':1}  }".Replace("'", "\"");
                var pagedQuery = Json.Deserialize<PagedQuery>(strPagedQuery);


                var filterService = new FilterService();
                filterService.getLeftValueExpression = (parameterExpression, rule) => NestedField_Extensions.GetFieldExpression(parameterExpression, rule.field);

                var pageData = userQuery.ToPageData(pagedQuery, filterService);

                Assert.AreEqual(2, pageData.totalCount);
                Assert.AreEqual(6, pageData.items[0].id);
            }

        }








    }
}
