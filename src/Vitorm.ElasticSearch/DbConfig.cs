using System;
using System.Collections.Generic;

namespace Vitorm.ElasticSearch
{
    public class DbConfig
    {
        public DbConfig(string connectionString, int? commandTimeout = null)
        {
            this.connectionString = connectionString;
            this.commandTimeout = commandTimeout;
        }
        public DbConfig(string connectionString, string readOnlyConnectionString, int? commandTimeout = null)
        {
            this.connectionString = connectionString;
            this.readOnlyConnectionString = readOnlyConnectionString;
            this.commandTimeout = commandTimeout;
        }
        public DbConfig(Dictionary<string, object> config)
        {
            object value;
            if (config.TryGetValue("connectionString", out value))
                this.connectionString = value as string;

            if (config.TryGetValue("readOnlyConnectionString", out value))
                this.readOnlyConnectionString = value as string;

            if (config.TryGetValue("commandTimeout", out value) && value is Int32 commandTimeout)
                this.commandTimeout = commandTimeout;


            if (config.TryGetValue("dateFormat", out value) && value is string str)
                this.dateFormat = str;

            if (config.TryGetValue("maxResultWindowSize", out value) && value is Int32 maxResultWindowSize)
                this.maxResultWindowSize = maxResultWindowSize;

            if (config.TryGetValue("track_total_hits", out value) && value is Boolean track_total_hits)
                this.track_total_hits = track_total_hits;
        }

        public string connectionString { get; set; }
        public string readOnlyConnectionString { get; set; }
        public int? commandTimeout { get; set; }

        public string dateFormat { get; set; }
        public int? maxResultWindowSize { get; set; }
        public bool? track_total_hits { get; set; }

        internal string dbHashCode => connectionString.GetHashCode().ToString();
    }
}
