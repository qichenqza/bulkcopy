using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkCopy
{
    public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> source, int size)
    {
        if(size <= 0) return null;

        while (source.Any())
        {
            yield return source.Take(size);
            source = source.Skip(size);
        }
    }

    public static class BulkOperator
    {
        public static string tmpSuffix = "TmpTable";
        public static Dictionary<string, string> csToSqlColumnMap = new Dictionary<string, string> {
            { "System.Int16","smallint"},
            { "System.Int32","int"},
            { "System.Int64", "bigint"},
            { "System.UInt16","smallint"},
            { "System.UInt32","int"},
            { "System.UInt64", "bigint"},
            { "System.Object", "sql_variant"},
            { "System.Boolean", "bit"},
            { "System.String", "nvarchar(max)"},
            { "System.Char[]", "nvarchar(max)"},
            { "System.DateTime", "datetime"},
            { "System.DateTimeOffset", "datetimeoffset"},
            { "System.Decimal", "decimal"},
            { "System.Double", "float"},
            { "System.TimeSpan", "time"},
            { "System.Byte[]", "varbinary"},
            { "System.SByte[]", "varbinary"},
            { "System.Single", "real"},
            { "System.Guid", "uniqueidentifier"},
            { "System.Byte", "tinyint"},
            { "System.SByte", "tinyint"},
            { "System.Char", "nchar"},
        };

        public class MappingPair
        {
            public string source { get; set; }
            public string destination { get; set; }

            public MappingPair()
            {

            }

            public MappingPair(string source, string destination)
            {
                this.source = source;
                this.destination = destination;
            }

            // return true when both source & destination are not null or empty
            public bool hasEmptyValue()
            {
                return string.IsNullOrEmpty(source) || string.IsNullOrEmpty(destination);
            }
        }

        public class MappingPairComparer : IEqualityComparer<MappingPair>
        {
            public bool Equals(MappingPair x, MappingPair y)
            {
                return x.destination == y.destination;
            }

            public int GetHashCode(MappingPair obj)
            {
                return obj.destination.GetHashCode();
            }
        }

        private static void initDataTableSchema<T>(DataTable table, List<string> schemaWhiteList = null)
        {
            // get all props of Class T
            PropertyDescriptorCollection props = TypeDescriptor.GetProperties(typeof(T));

            foreach (PropertyDescriptor prop in props)
            {
                // check schema white list
                if ((schemaWhiteList?.Count ?? 0) == 0 || schemaWhiteList.Contains(prop.Name))
                {
                    table.Columns.Add(prop.Name, prop.PropertyType);
                }
            }
        }

        private static void checkDataTable(DataTable table)
        {
            // check DataTable, throw excaption if schema of table is empty
            if ((table?.Columns?.Count ?? 0) == 0)
            {
                throw new Exception("The schema of DataTable is empty after init;");
            }
        }

        private static void checkMappingPairList(List<MappingPair> mappingPairs)
        {
            // check MappingPair list, throw exception if contains no element or contains any empty element
            if ((mappingPairs?.Count ?? 0) == 0 || mappingPairs.Find((t) => { return t.hasEmptyValue(); }) != null)
            {
                throw new Exception("The List of MappingPair is empty or contains empty value;");
            }
        }

        public static DataTable getDataTable<T>(IList<T> data, DataTable table = null, List<string> schemaWhiteList = null)
        {
            // get data table from data list
            table = table ?? new DataTable();
            initDataTableSchema<T>(table, schemaWhiteList: schemaWhiteList);

            // check shema of data table
            checkDataTable(table);

            try
            {
                // get all schema of Class T
                PropertyDescriptorCollection props =
                    TypeDescriptor.GetProperties(typeof(T));

                // init row values
                object[] values = new object[table.Columns.Count];

                foreach (T item in data)
                {
                    int j = 0;
                    foreach (PropertyDescriptor prop in props)
                    {
                        // check schema white list
                        if ((schemaWhiteList?.Count ?? 0) == 0 || schemaWhiteList.Contains(prop.Name))
                        {
                            values[j] = prop.GetValue(item);
                            j++;
                        }
                    }

                    // add rows
                    table.Rows.Add(values);
                }
            }
            catch (Exception)
            {
                table.Clear();
                throw;
            }

            return table;
        }

        public static async Task bulkInsert<T>(SqlConnection conn, string tableName, IList<T> data, int batchSize = 5000, List<MappingPair> mappingList = null, SqlTransaction externalTransaction = null)
        {
            // get data table from data list
            DataTable dataTable = getDataTable<T>(data, schemaWhiteList: mappingList?.ConvertAll((t) => { return t.source; }));

            try
            {
                // check schema of data table
                checkDataTable(dataTable);

                // init bulk copy
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.TableLock, externalTransaction: externalTransaction))
                {
                    // set destination table name
                    bulkCopy.DestinationTableName = tableName;
                    // set batch size, bulk copy with lock table every batch
                    bulkCopy.BatchSize = batchSize;

                    // init mapping list from data table to sql table
                    if (mappingList == null)
                    {
                        mappingList = new List<MappingPair>();
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            string columnName = column.ColumnName;
                            mappingList.Add(new MappingPair(columnName, columnName));
                        }
                    }

                    // check mapping list
                    checkMappingPairList(mappingList);
                    // unique mapping list
                    mappingList = mappingList.Distinct(new MappingPairComparer()).ToList();

                    // set mapping list for bulk copy
                    foreach (MappingPair mapping in mappingList)
                    {
                        bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(mapping.source, mapping.destination));
                    }

                    // write data table to sql table
                    await bulkCopy.WriteToServerAsync(dataTable);
                }
            }
            catch (Exception)
            {
                // TODO: handle exception
                throw;
            }
            finally
            {
                // finally clear data table
                dataTable.Clear();
            }

        }

        private static string getTmpTableName(string tableName)
        {
            return $"#{tableName}_{Guid.NewGuid():N}_{tmpSuffix}";
        }

        private static string getCreateTableSql<T>(string tmpTableName, List<string> schemaWhiteList = null)
        {
            // get all schema of Class T
            PropertyDescriptorCollection props = TypeDescriptor.GetProperties(typeof(T));

            StringBuilder createTableSql = new StringBuilder();
            List<string> columnList = new List<string>();

            // add sql statement
            createTableSql.Append($"drop table if exists [{tmpTableName}];");
            createTableSql.Append($"create table [{tmpTableName}]( ");

            foreach (PropertyDescriptor prop in props)
            {
                if ((schemaWhiteList?.Count ?? 0) == 0 || schemaWhiteList.Contains(prop.Name))
                {
                    // check if property type is nullable
                    var propertyType = prop.PropertyType;
                    if (propertyType.IsGenericType && propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        propertyType = propertyType.GetGenericArguments()[0];
                    }
                    // trans data type from CS to sql
                    if (csToSqlColumnMap.TryGetValue(propertyType.FullName, out string columnType)) columnList.Add($"[{prop.Name}] {columnType}");
                }
            }

            // check column list
            if ((columnList?.Count ?? 0) == 0) throw new Exception($"Can not get schema of table: {tmpTableName} when creating table;");

            createTableSql.Append(string.Join(",", columnList));
            createTableSql.Append(" );");
            return createTableSql.ToString();
        }

        private static async Task bulkMerge<T>(SqlConnection conn, string tmpTableName, string tmpTableSql, string mergeSql, IList<T> data, List<string> schemaWhiteList = null, SqlTransaction externalTransaction = null)
        {
            // init command & set timeout
            SqlCommand command = conn.CreateCommand();
            command.CommandTimeout = 0;
            // start a new transaction if external is null
            SqlTransaction transaction = externalTransaction == null ? conn.BeginTransaction(iso: IsolationLevel.RepeatableRead) : null;

            // init sql command
            command.Connection = conn;
            command.Transaction = transaction ?? externalTransaction;

            try
            {
                // create tmp table
                command.CommandText = tmpTableSql;
                await command.ExecuteNonQueryAsync();

                // bulk copy into tmp table
                await bulkInsert<T>(conn, tmpTableName, data, externalTransaction: externalTransaction ?? transaction, mappingList: schemaWhiteList?.ConvertAll((t) => { return new MappingPair(t, t); }));
                // merge target table & source table
                command.CommandText = mergeSql;
                await command.ExecuteNonQueryAsync();

                // drop tmp table
                command.CommandText = $"drop table {tmpTableName}";
                await command.ExecuteNonQueryAsync();

                // commit transaction
                transaction?.Commit();
            }
            catch (Exception)
            {
                // roll back
                transaction?.Rollback();
                throw;
            }
        }

        private static List<MappingPair> initMappingPairList<T>()
        {
            List<MappingPair> mappingPairs = new List<MappingPair>();
            PropertyDescriptorCollection props = TypeDescriptor.GetProperties(typeof(T));
            for (int i = 0; i < props.Count; i++)
            {
                PropertyDescriptor prop = props[i];
                mappingPairs.Add(new MappingPair(prop.Name, prop.Name));
            }
            return mappingPairs;
        }

        public static async Task bulkUpdate<T>(SqlConnection conn, string targetTableName, IList<T> data, int concurrencyLevel, List<MappingPair> keyMappingList, List<MappingPair> valueMappingList = null, SqlTransaction externalTransaction = null)
        {
            List<Task> taskList = new List<Task>();
            foreach (var tmpDataList in data.Batch((int)Math.Ceiling(data.Count / (decimal)concurrencyLevel)))
            {
                taskList.Add(bulkUpdate(conn, targetTableName, tmpDataList.ToList(), keyMappingList, valueMappingList, externalTransaction));
            }
            await Task.WhenAll(taskList.ToArray());
        }

        public static async Task bulkUpdate<T>(SqlConnection conn, string targetTableName, IList<T> data, List<MappingPair> keyMappingList, List<MappingPair> valueMappingList = null, SqlTransaction externalTransaction = null)
        {
            // init value mapping list if null
            valueMappingList = valueMappingList ?? initMappingPairList<T>();

            // check input key mapping & value mapping list
            checkMappingPairList(keyMappingList);
            checkMappingPairList(valueMappingList);

            // filter schema
            List<string> schemaWhiteList = keyMappingList.ConvertAll((t) => { return t.source; });
            schemaWhiteList.AddRange(valueMappingList.ConvertAll((t) => { return t.source; }));
            schemaWhiteList = schemaWhiteList.Distinct().ToList();

            // get tmp table name
            string tmpTableName = getTmpTableName(targetTableName);
            // get create sql statement based on Class T
            string tmpTableSql = getCreateTableSql<T>(tmpTableName, schemaWhiteList: schemaWhiteList);

            StringBuilder mergeSql = new StringBuilder();
            // start format merge statement
            mergeSql.Append($"merge [{targetTableName}] as target ");
            mergeSql.Append($"using (select * from [{tmpTableName}]) as source on ");

            // use item in key mapping list to match
            mergeSql.Append($"target.[{keyMappingList[0].destination}] = source.[{keyMappingList[0].source}] ");
            for (int i = 1; i < keyMappingList.Count; i++)
            {
                mergeSql.Append($"and target.[{keyMappingList[i].destination}] = source.[{keyMappingList[i].source}] ");
            }
            mergeSql.Append($"when matched then ");

            // use item in value mapping list to update
            mergeSql.Append($"update set target.[{valueMappingList[0].destination}] = source.[{valueMappingList[0].source}] ");
            for (int i = 1; i < valueMappingList.Count; i++)
            {
                mergeSql.Append($",target.[{valueMappingList[i].destination}] = source.[{valueMappingList[i].source}] ");
            }
            mergeSql.Append(" ;");

            // start bulk merge
            await bulkMerge<T>(conn, tmpTableName, tmpTableSql, mergeSql.ToString(), data, schemaWhiteList: schemaWhiteList, externalTransaction: externalTransaction);
        }

        public static async Task bulkDelete<T>(SqlConnection conn, string targetTableName, IList<T> data, int concurrencyLevel, List<MappingPair> keyMappingList, SqlTransaction externalTransaction = null)
        {
            List<Task> taskList = new List<Task>();
            foreach (var tmpDataList in data.Batch((int)Math.Ceiling(data.Count / (decimal)concurrencyLevel)))
            {
                taskList.Add(bulkDelete(conn, targetTableName, tmpDataList.ToList(), keyMappingList, externalTransaction));
            }
            await Task.WhenAll(taskList.ToArray());
        }

        public static async Task bulkDelete<T>(SqlConnection conn, string targetTableName, IList<T> data, List<MappingPair> keyMappingList, SqlTransaction externalTransaction = null)
        {
            // check input key mapping list
            checkMappingPairList(keyMappingList);

            // get filter list
            List<string> schemaWhiteList = keyMappingList.ConvertAll((t) => { return t.source; });

            // get tmp table name
            string tmpTableName = getTmpTableName(targetTableName);
            // get create table sql statement
            string tmpTableSql = getCreateTableSql<T>(tmpTableName, schemaWhiteList: schemaWhiteList);

            StringBuilder mergeSql = new StringBuilder();
            mergeSql.Append($"merge [{targetTableName}] as target ");
            mergeSql.Append($"using (select * from [{tmpTableName}]) as source on ");
            mergeSql.Append($"target.[{keyMappingList[0].destination}] = source.[{keyMappingList[0].source}] ");
            for (int i = 1; i < keyMappingList.Count; i++)
            {
                mergeSql.Append($"and target.[{keyMappingList[i].destination}] = source.[{keyMappingList[i].source}] ");
            }
            mergeSql.Append($"when matched then ");
            mergeSql.Append($"delete;");
            await bulkMerge(conn, tmpTableName, tmpTableSql, mergeSql.ToString(), data, schemaWhiteList: schemaWhiteList, externalTransaction: externalTransaction);
        }
    }
}
