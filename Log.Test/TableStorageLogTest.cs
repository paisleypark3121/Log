using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage.Table;

namespace Log.Test
{
    [TestClass]
    public class TableStorageLogTest
    {
        [TestMethod]
        public void InsertTest()
        {
            #region arrange
            string[] log_args = new string[4] {
                "CONNECTION_STRING",
                "TABLE_NAME",
                "LogName",
                "internal_id"
            };
            Dictionary<string, string> _internal_parameters = new Dictionary<string, string>();
            _internal_parameters.Add("param1", "value_param1");
            _internal_parameters.Add("param2", "value_param2");
            #endregion

            #region act
            ILog log = new Log.TableStorageLog(log_args);
            LogEntry _logEntry = new LogEntry
            {
                LogName = "TestLog",
                internal_id = "1234",
                requestTime = DateTime.Now.AddSeconds(-10),
                responseTime = DateTime.Now,
                request = "myrequest",
                response = "myresponse",
                internal_parameters = JsonConvert.SerializeObject(_internal_parameters)
            };
            log.Track(_logEntry);
            #endregion

            #region assert
            Assert.IsTrue(true);
            #endregion
        }

        [TestMethod]
        public void InsertGetDeleteTableStorageWithPkAndRkTest()
        {
            #region insert
            #region arrange
            bool expected = true;
            string[] log_args = new string[4] {
                "UseDevelopmentStorage=true",
                "TestTable",
                "LogName",
                "internal_id"
            };
            Dictionary<string, string> _internal_parameters = new Dictionary<string, string>();
            _internal_parameters.Add("param1", "value_param1");
            _internal_parameters.Add("param2", "value_param2");

            string partitionKey = "MyTestTable";
            string rowKey = Log.Utilities.getInvertedTimeKey(DateTime.Now);
            #endregion

            #region act
            ILog log = new TableStorageLog(log_args);
            LogEntry _logEntry = new LogEntry
            {
                LogName = partitionKey,
                internal_id = rowKey,
                requestTime = DateTime.Now.AddSeconds(-10),
                responseTime = DateTime.Now,
                request = "myrequest",
                response = "myresponse",
                internal_parameters = JsonConvert.SerializeObject(_internal_parameters)
            };
            bool actual=log.Track(_logEntry);
            #endregion

            #region assert
            Assert.AreEqual(expected, actual);
            #endregion

            #endregion

            #region get

            #region act
            TableStorageLog TSlog = (TableStorageLog)log;
            TableStorageEntry entry=TSlog.Get(partitionKey, rowKey);
            LogEntry actualLogEntry = entry.ToEntity<LogEntry>("LogName", "internal_id");
            #endregion

            #region assert
            Assert.AreEqual(_logEntry.LogName, actualLogEntry.LogName);
            Assert.AreEqual(_logEntry.internal_id, actualLogEntry.internal_id);
            Assert.AreEqual(_logEntry.request, actualLogEntry.request);
            Assert.AreEqual(_logEntry.response, actualLogEntry.response);
            Assert.AreEqual(_logEntry.requestTime, actualLogEntry.requestTime);
            Assert.AreEqual(_logEntry.responseTime, actualLogEntry.responseTime);
            Assert.AreEqual(_logEntry.internal_parameters, actualLogEntry.internal_parameters);
            #endregion

            #endregion

            #region delete

            #endregion
        }

        [TestMethod]
        public void InsertWithPkOrderedTest()
        {
            #region arrange
            bool expected = true;
            string[] log_args = new string[4] {
                "UseDevelopmentStorage=true",
                "TestTable",
                "LogName",
                "internal_id"
            };
            Dictionary<string, string> _internal_parameters = new Dictionary<string, string>();
            _internal_parameters.Add("param1", "value_param1");
            _internal_parameters.Add("param2", "value_param2");

            string partitionKey = "MyTestTable";
            string rowKey = Log.Utilities.getInvertedTimeKey(DateTime.Now);
            #endregion

            #region act
            ILog log = new TableStorageLog(log_args);
            LogEntry _logEntry = new LogEntry
            {
                LogName = partitionKey,
                internal_id = rowKey,
                requestTime = DateTime.Now.AddSeconds(-10),
                responseTime = DateTime.Now,
                request = "myrequest",
                response = "myresponse",
                internal_parameters = JsonConvert.SerializeObject(_internal_parameters)
            };
            bool actual = log.Track(_logEntry);
            #endregion

            #region assert
            Assert.AreEqual(expected, actual);
            #endregion
        }

        [TestMethod]
        public void GetElementByPkAndRkTest()
        {
            #region arrange
            string[] log_args = new string[4] {
                "UseDevelopmentStorage=true",
                "TestTable",
                "LogName",
                "internal_id"
            };
            string partitionKey = "MyTestTable";
            string rowKey = "2518152595287108267";
            #endregion

            #region act
            TableStorageLog TSlog = new TableStorageLog(log_args);
            TableStorageEntry entry = TSlog.Get(partitionKey, rowKey);
            LogEntry actualLogEntry = entry.ToEntity<LogEntry>("LogName", "internal_id");
            #endregion

            #region assert
            Assert.IsNotNull(actualLogEntry);
            #endregion
        }

        [TestMethod]
        public void GetElementByPkFromToTest()
        {
            #region arrange
            string[] log_args = new string[4] {
                "UseDevelopmentStorage=true",
                "TestTable",
                "LogName",
                "internal_id"
            };
            string partitionKey = "MyTestTable";
            DateTime startDate = DateTime.Now.AddHours(-5);
            DateTime endDate = DateTime.Now;
            #endregion

            #region act
            TableStorageLog TSlog = new TableStorageLog(log_args);
            List<TableStorageEntry> entries = TSlog.Get(partitionKey,startDate,endDate);
            #endregion

            #region assert
            Assert.IsNotNull(entries);
            Assert.IsTrue(entries.Count == 1);
            #endregion
        }

        [TestMethod]
        public void DeleteByPkTest()
        {
            #region arrange
            bool expected = true;
            string[] log_args = new string[4] {
                "UseDevelopmentStorage=true",
                "TestTable",
                "LogName",
                "internal_id"
            };
            string partitionKey = "MyTestTable";
            string rowKey = "2518152482955735696";
            #endregion

            #region act
            TableStorageLog TSlog = new TableStorageLog(log_args);
            bool actual = TSlog.UnTrack(partitionKey, rowKey);
            #endregion

            #region assert
            Assert.AreEqual(expected,actual);
            #endregion
        }

        [TestMethod]
        public void InsertRkOrderedMockTest()
        {
            #region arrange
            bool expected = true;
            string[] log_args = new string[4] {
                "CONNECTION_STRING",
                "TABLE_NAME",
                "LogName",
                "internal_id"
            };
            Dictionary<string, string> _internal_parameters = new Dictionary<string, string>();
            _internal_parameters.Add("param1", "value_param1");
            _internal_parameters.Add("param2", "value_param2");
            #endregion

            #region act
            //ILog log = new TableStorageLog(log_args);

            Mock<ILog> mock_Log = new Moq.Mock<ILog>();
            mock_Log.Setup(x => x.Track(It.IsAny<object>())).Equals(true);
            LogEntry _logEntry = new LogEntry
            {
                LogName = "MyTestRkLog",
                //internal_id = "1234",
                internal_id = Log.Utilities.getInvertedTimeKey(DateTime.Now),
                requestTime = DateTime.Now.AddSeconds(-10),
                responseTime = DateTime.Now,
                request = "myrequest",
                response = "myresponse",
                internal_parameters = JsonConvert.SerializeObject(_internal_parameters)
            };
            bool actual=mock_Log.Object.Track(_logEntry);
            #endregion

            #region assert
            Assert.AreEqual(expected, actual);
            #endregion
        }
    }
}
