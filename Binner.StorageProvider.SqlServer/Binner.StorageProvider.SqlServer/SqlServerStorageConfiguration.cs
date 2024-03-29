﻿using System.Collections.Generic;

namespace Binner.StorageProvider.SqlServer
{
    public class SqlServerStorageConfiguration
    {
        public string ConnectionString { get; set; } = string.Empty;

        public SqlServerStorageConfiguration()
        {
        }

        public SqlServerStorageConfiguration(IDictionary<string, string> config)
        {
            if (config.ContainsKey("ConnectionString"))
                ConnectionString = config["ConnectionString"];
        }
    }
}
