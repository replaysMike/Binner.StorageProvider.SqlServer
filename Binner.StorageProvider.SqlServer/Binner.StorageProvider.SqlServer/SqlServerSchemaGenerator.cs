using Binner.Model.Common;
using System.ComponentModel.DataAnnotations;
using TypeSupport;
using TypeSupport.Extensions;

namespace Binner.StorageProvider.SqlServer
{
    public class SqlServerSchemaGenerator<T>
    {
        private string _dbName;
        private ICollection<ExtendedProperty> _tables;

        public SqlServerSchemaGenerator(string databaseName)
        {
            _dbName = databaseName;
            var properties = typeof(T).GetProperties(PropertyOptions.HasGetter);
            _tables = properties.Where(x => x.Type.IsCollection).ToList();
        }

        public string CreateDatabaseIfNotExists()
        {
            return $@"
DECLARE @dbCreated INT = 0;
IF (db_id(N'{_dbName}') IS NULL)
BEGIN
    CREATE DATABASE {_dbName};
    SET @dbCreated = 1;
END
SELECT @dbCreated;
";
        }

        public string CreateTableSchemaIfNotExists()
        {
            return $@"
USE {_dbName};
-- create tables
DECLARE @tablesCreated INT = 0;
{string.Join("\r\n", GetTableSchemas())}
SELECT @tablesCreated;
";
        }

        private ICollection<string> GetTableSchemas()
        {
            var tableSchemas = new List<string>();
            foreach (var tableProperty in _tables)
            {
                var tableExtendedType = tableProperty.Type;
                var columnProps = tableExtendedType.ElementType.GetProperties(PropertyOptions.HasGetter);
                var columnSchema = new List<string>();
                foreach (var columnProp in columnProps)
                    columnSchema.Add(GetColumnSchema(columnProp));

                tableSchemas.Add(CreateTableIfNotExists(tableProperty.Name, string.Join(",\r\n", columnSchema)));
                // also add schema new columns added
                foreach (var columnProp in columnProps)
                    tableSchemas.Add(CreateTableColumnIfNotExists(tableProperty.Name, columnProp));
            }

            return tableSchemas;
        }

        private string GetColumnSchema(ExtendedProperty prop, bool includeDefaultValue = false)
        {
            var columnSchema = "";
            var defaultValue = "";
            var propExtendedType = prop.Type;
            var maxLength = GetMaxLength(prop);
            if (propExtendedType.IsCollection)
            {
                // store as string, data will be comma delimited
                columnSchema = $"{prop.Name} nvarchar({maxLength})";
                defaultValue = "''";
            }
            else
            {
                switch (propExtendedType)
                {
                    case var p when p.NullableBaseType == typeof(byte):
                        columnSchema = $"{prop.Name} tinyint";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(short):
                        columnSchema = $"{prop.Name} smallint";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(int):
                        columnSchema = $"{prop.Name} integer";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(long):
                        columnSchema = $"{prop.Name} bigint";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(double):
                        columnSchema = $"{prop.Name} float";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(decimal):
                        columnSchema = $"{prop.Name} decimal(18, 3)";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(string):
                        columnSchema = $"{prop.Name} nvarchar({maxLength})";
                        defaultValue = "''";
                        break;
                    case var p when p.NullableBaseType == typeof(DateTime):
                        columnSchema = $"{prop.Name} datetime";
                        defaultValue = "GETUTCDATE()";
                        break;
                    case var p when p.NullableBaseType == typeof(TimeSpan):
                        columnSchema = $"{prop.Name} time";
                        defaultValue = "GETUTCDATE()";
                        break;
                    case var p when p.NullableBaseType == typeof(Guid):
                        columnSchema = $"{prop.Name} uniqueidentifier";
                        defaultValue = "NEWID()";
                        break;
                    case var p when p.NullableBaseType == typeof(bool):
                        columnSchema = $"{prop.Name} bit";
                        defaultValue = "0";
                        break;
                    case var p when p.NullableBaseType == typeof(byte[]):
                        columnSchema = $"{prop.Name} varbinary({maxLength})";
                        defaultValue = "convert(varbinary(1), '')";
                        break;
                    case var p when p.NullableBaseType.IsEnum:
                        columnSchema = $"{prop.Name} integer";
                        defaultValue = "0";
                        break;
                    default:
                        throw new StorageProviderException(nameof(SqlServerStorageProvider), $"Unsupported data type: {prop.Type}");
                }
            }
            if (prop.CustomAttributes.ToList().Any(x => x.AttributeType == typeof(KeyAttribute)))
            {
                if (propExtendedType.NullableBaseType != typeof(string) && propExtendedType.NullableBaseType.IsValueType)
                    columnSchema += " IDENTITY";
                columnSchema += $" PRIMARY KEY NOT NULL";
                if (includeDefaultValue)
                    columnSchema += $" DEFAULT {defaultValue}";
            }
            else if (propExtendedType.Type != typeof(string) && !propExtendedType.IsNullable &&
                     !propExtendedType.IsCollection)
            {
                columnSchema += $" NOT NULL";
                if (includeDefaultValue)
                    columnSchema += $" DEFAULT {defaultValue}";
            }

            return columnSchema;
        }

        private string GetMaxLength(ExtendedProperty prop)
        {
            var maxLengthAttr = prop.CustomAttributes.ToList().FirstOrDefault(x => x.AttributeType == typeof(MaxLengthAttribute));
            var maxLength = "max";
            if (maxLengthAttr != null)
            {
                maxLength = maxLengthAttr.ConstructorArguments.First().Value?.ToString() ?? "max";
            }
            return maxLength;
        }

        private string CreateTableColumnIfNotExists(string tableName, ExtendedProperty columnProp)
        {
            var columnSchema = GetColumnSchema(columnProp, true);
            return $@"IF NOT EXISTS (SELECT c.* FROM sysobjects o JOIN syscolumns c ON c.id=o.id WHERE o.name='{tableName}' and o.xtype='U' and c.name='{columnProp.Name}')
BEGIN
    ALTER TABLE {tableName} ADD {columnSchema};
END";
        }

        private string CreateTableIfNotExists(string tableName, string tableSchema)
        {
            return $@"IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{tableName}' and xtype='U')
BEGIN
    CREATE TABLE {tableName} (
        {tableSchema}
    );
   SET @tablesCreated = @tablesCreated + 1;
END";
        }
    }
}
