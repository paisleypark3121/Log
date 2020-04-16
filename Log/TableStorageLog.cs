using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Log
{
    public class TableStorageLog : ILog
    {
        #region variables
        private string connectionString = null;
        private string resourceName = null;
        private string partitionKey = null;
        private string rowKey = null;
        #endregion

        #region storage management
        private CloudStorageAccount storageAccount = null;
        private CloudTableClient tableClient = null;
        private CloudTable table = null;
        #endregion

        public TableStorageLog() { }

        public TableStorageLog(string[] args)
        {
            #region preconditions
            if ((args == null) ||
                (args.Length < 3))
                throw new Exception("Missing mandatory parameters args");
            #endregion

            #region Manage variables
            connectionString = args[0].ToString();
            resourceName = args[1].ToString();
            partitionKey = args[2].ToString();
            if (args.Length==4)
                rowKey = args[3].ToString();
            #endregion

            #region preconditions
            if (string.IsNullOrEmpty(connectionString) ||
                string.IsNullOrEmpty(resourceName) ||
                string.IsNullOrEmpty(partitionKey))
                throw new Exception("Missing mandatory parameters");
            #endregion

            #region manage table
            storageAccount = CloudStorageAccount.Parse(connectionString);
            tableClient = storageAccount.CreateCloudTableClient();
            table = tableClient.GetTableReference(resourceName);
            table.CreateIfNotExists();
            #endregion
        }

        #region ILOG
        public bool Track(object object2track)
        {
            #region try
            try
            {
                #region retry management
                int maxNumRetries = 5;
                int numRetries = 0;
                Random random = new Random();
                #endregion

                TableStorageEntry table_entry = new TableStorageEntry(partitionKey, rowKey, object2track);

                #region insert management
                while (numRetries < maxNumRetries)
                {
                    #region try
                    try
                    {
                        TableOperation insertOperation = TableOperation.Insert(table_entry);
                        table.Execute(insertOperation);

                        break;
                    }
                    #endregion
                    #region catch
                    catch (StorageException ex)
                    {
                        string error = "Error in function " + System.Reflection.MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                        System.Diagnostics.Trace.TraceError(error);
                        if (ex.Message.Contains("(409)"))
                        {
                            numRetries++;
                            long result = 0;
                            if (long.TryParse(table_entry.RowKey, out result))
                            {
                                if (numRetries >= 3)
                                    result += random.Next(2, 10);
                                else
                                    result++;
                                table_entry.RowKey = result.ToString();
                            }
                            else
                                break;
                        }
                        else
                            break;
                    }
                    catch (Exception ex)
                    {
                        string error = "Error in function " + System.Reflection.MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                        System.Diagnostics.Trace.TraceError(error);
                        return false;
                    }
                    #endregion
                }
                #endregion

                return true;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                string error = "Error in function " + System.Reflection.MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                System.Diagnostics.Trace.TraceError(error);
                return false;
            }
            #endregion
        }

        public async Task<bool> TrackAsync(object object2track)
        {
            #region try
            try
            {
                #region retry management
                int maxNumRetries = 5;
                int numRetries = 0;
                Random random = new Random();
                #endregion

                TableStorageEntry table_entry = new TableStorageEntry(partitionKey, rowKey, object2track);

                #region insert management
                while (numRetries < maxNumRetries)
                {
                    #region try
                    try
                    {
                        TableOperation insertOperation = TableOperation.Insert(table_entry);
                        await table.ExecuteAsync(insertOperation);

                        break;
                    }
                    #endregion
                    #region catch
                    catch (StorageException ex)
                    {
                        string error = "Error in function " + System.Reflection.MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                        System.Diagnostics.Trace.TraceError(error);
                        if (ex.Message.Contains("(409)"))
                        {
                            numRetries++;
                            long result = 0;
                            if (long.TryParse(table_entry.RowKey, out result))
                            {
                                if (numRetries >= 3)
                                    result += random.Next(2, 10);
                                else
                                    result++;
                                table_entry.RowKey = result.ToString();
                            }
                            else
                                break;
                        }
                        else
                            break;
                    }
                    catch (Exception ex)
                    {
                        string error = "Error in function " + System.Reflection.MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                        System.Diagnostics.Trace.TraceError(error);
                        return false;
                    }
                    #endregion
                }
                #endregion

                return true;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                string error = "Error in function " + System.Reflection.MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                System.Diagnostics.Trace.TraceError(error);
                return false;
            }
            #endregion
        }
        #endregion

        #region utilities
        public TableStorageEntry Get(string pk, string rk)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk) || string.IsNullOrEmpty(rk))
                    throw new Exception("Invalid parameters");
                #endregion

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rk)));

                TableStorageEntry _entry = table.ExecuteQuery(rangeQuery).FirstOrDefault<TableStorageEntry>();
                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public List<TableStorageEntry> Get(string pk, DateTime date)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk))
                    throw new Exception("Invalid parameters");
                #endregion

                string endRowKey = (DateTime.MaxValue.Ticks - date.Date.Ticks).ToString();
                string startRowKey = (DateTime.MaxValue.Ticks - date.Date.AddDays(1).Ticks).ToString();

                string partitionKeyFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk);
                string rowUpFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, endRowKey);
                string rowDownFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, startRowKey);

                string filter = TableQuery.CombineFilters(rowDownFilter, TableOperators.And, rowUpFilter);
                filter = TableQuery.CombineFilters(filter, TableOperators.And, partitionKeyFilter);

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(filter);
                List<TableStorageEntry> _entry = table.ExecuteQuery(rangeQuery).ToList<TableStorageEntry>();

                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public List<TableStorageEntry> Get(string pk, DateTime fromDate, DateTime toDate)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk))
                    throw new Exception("Invalid parameters");
                #endregion

                string endRowKey = (DateTime.MaxValue.Ticks - fromDate.Ticks).ToString();
                string startRowKey = (DateTime.MaxValue.Ticks - toDate.Ticks).ToString();

                string partitionKeyFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk);
                string rowUpFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, endRowKey);
                string rowDownFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, startRowKey);

                string filter = TableQuery.CombineFilters(rowDownFilter, TableOperators.And, rowUpFilter);
                filter = TableQuery.CombineFilters(filter, TableOperators.And, partitionKeyFilter);

                TableQuery<TableStorageEntry> rangeQuery = new TableQuery<TableStorageEntry>().Where(filter);
                List<TableStorageEntry> _entry = table.ExecuteQuery(rangeQuery).ToList<TableStorageEntry>();

                return _entry;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return null;
            }
            #endregion
        }

        public bool UnTrack(string pk, string rk)
        {
            #region try
            try
            {
                #region preconditions
                if (string.IsNullOrEmpty(pk) || string.IsNullOrEmpty(rk))
                    throw new Exception("Invalid parameters");
                #endregion

                TableOperation retrieve = TableOperation.Retrieve(pk, rk);
                TableResult result = table.Execute(retrieve);

                if (result.Result != null)
                {
                    TableOperation delete = TableOperation.Delete((ITableEntity)result.Result);
                    table.Execute(delete);
                }

                return true;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError(ex.Message);
                return false;
            }
            #endregion
        }
        #endregion

    }
}
