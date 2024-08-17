using System;
using System.Threading.Tasks;

namespace Vitorm.ElasticSearch
{
    public partial class DbContext : Vitorm.DbContext
    {

        #region #1.2 Schema :  Truncate
        public override void Truncate<Entity>()
        {
            throw new NotImplementedException();
        }
        public override Task TruncateAsync<Entity>()
        {
            throw new NotImplementedException();
        }
        #endregion


    }
}
