using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Web;

namespace Log
{
    public class TableStorageEntry : TableEntity
    {
        private IDictionary<string, EntityProperty> properties;

        public TableStorageEntry(
            string _partitionKey,
            string _rowKey,
            object o)
        {
            #region preconditions
            if (o.GetType().GetProperty(_partitionKey) == null)
                throw new Exception("Missing partitionKey " + _partitionKey + " in object");
            #endregion

            #region rowKey preconditions
            string rowkey = null;
            if (string.IsNullOrEmpty(_rowKey))
            {
                rowkey = Utilities.getInvertedTimeKey(DateTime.Now).ToString();
                this.RowKey = HttpUtility.UrlEncode(rowkey);
            } else if (o.GetType().GetProperty(_rowKey) == null)
                throw new Exception("Missing rowKey " + _rowKey + " in object");
            #endregion

            #region timestamp
            this.Timestamp = DateTime.Now;
            properties = new Dictionary<string, EntityProperty>();
            #endregion

            #region properties
            PropertyInfo[] _elements = o.GetType().GetProperties();
            foreach (PropertyInfo _property in _elements)
            {
                #region property: name - value
                object value = _property.GetValue(o);
                if (value == null)
                    continue;
                string name = _property.Name;
                #endregion

                #region PartitionKey
                if (_property.Name == _partitionKey)
                {
                    name = "PartitionKey";
                    this.PartitionKey = HttpUtility.UrlEncode(value.ToString());
                    continue;
                }
                #endregion

                #region RowKey
                if (_property.Name == _rowKey)
                {
                    name = "RowKey";
                    this.RowKey = HttpUtility.UrlEncode(value.ToString());
                    continue;
                }
                #endregion

                if (value.GetType() == typeof(string))
                    value = HttpUtility.UrlEncode(value.ToString());
                AddValue(name, value, properties);
            }
            #endregion
        }

        //public int VerbosityLevel { get; set;}
        public TableStorageEntry() { }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return properties;
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> _properties, OperationContext operationContext)
        {
            this.properties = _properties;
        }

        internal void AddValue(string name, object value, IDictionary<string, EntityProperty> properties)
        {
            if (value.GetType() == typeof(string))
                properties.Add(name, new EntityProperty((string)value));
            if (value.GetType() == typeof(int))
                properties.Add(name, new EntityProperty((int)value));
            if (value.GetType() == typeof(DateTime))
                properties.Add(name, new EntityProperty((DateTime)value));
            if (value.GetType() == typeof(bool))
                properties.Add(name, new EntityProperty((bool)value));
            if (value.GetType() == typeof(double))
                properties.Add(name, new EntityProperty((double)value));
            if (value.GetType() == typeof(short))
                properties.Add(name, new EntityProperty((short)value));
            if (value.GetType() == typeof(float))
                properties.Add(name, new EntityProperty((float)value));
            if (value.GetType() == typeof(char))
                properties.Add(name, new EntityProperty((char)value));
            if (value.GetType() == typeof(byte))
                properties.Add(name, new EntityProperty((byte)value));
            if (value.GetType() == typeof(long))
                properties.Add(name, new EntityProperty((long)value));
        }

        internal void SetValue(object entity, PropertyInfo property,object value)
        {
            EdmType _type = ((EntityProperty)value).PropertyType;

            if (_type == EdmType.String)
                property.SetValue(entity, ((EntityProperty)value).StringValue);
            else if (_type == EdmType.Int32)
                property.SetValue(entity, ((EntityProperty)value).Int32Value);
            else if (_type == EdmType.Int64)
                property.SetValue(entity, ((EntityProperty)value).Int64Value);
            else if (_type == EdmType.DateTime)
                property.SetValue(entity, ((EntityProperty)value).DateTime);
            else if (_type == EdmType.Boolean)
                property.SetValue(entity, ((EntityProperty)value).BooleanValue);
            else if (_type == EdmType.Double)
                property.SetValue(entity, ((EntityProperty)value).DoubleValue);
        }

        public T ToEntity<T>(
            string PartitionKey,
            string RowKey) where T : class, new()
        {
            #region try
            try
            {
                T entity = new T();

                #region preconditions
                if ((string.IsNullOrEmpty(PartitionKey)) || (string.IsNullOrEmpty(RowKey)))
                    throw new Exception("Missing mandatory parameters Partition / Row keys");
                #endregion

                #region properties
                PropertyInfo[] _elements = entity.GetType().GetProperties();
                foreach (PropertyInfo _property in _elements)
                {
                    #region PartitionKey
                    if (_property.Name == PartitionKey)
                    {
                        _property.SetValue(entity, this.PartitionKey);
                        continue;
                    }
                    #endregion

                    #region RowKey
                    if (_property.Name == RowKey)
                    {
                        _property.SetValue(entity, this.RowKey);
                        continue;
                    }
                    #endregion

                    this.SetValue(entity, _property, this.properties[_property.Name]);
                    //_property.SetValue(entity, this.properties[_property.Name]);
                }
                #endregion

                return entity;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                string error = "Error in function " + MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                System.Diagnostics.Trace.TraceError(error);
                return null;
            }
            #endregion
        }

        public static T ToEntity<T>(
            DynamicTableEntity _tableEntity,
            string PartitionKey,
            string RowKey) where T : class, new()
        {
            #region try
            try
            {
                T entity = new T();

                #region preconditions
                if (_tableEntity == null)
                    return default(T);
                if ((string.IsNullOrEmpty(PartitionKey)) || (string.IsNullOrEmpty(RowKey)))
                    throw new Exception("Missing mandatory parameters Partition / Row keys");
                #endregion

                #region properties
                PropertyInfo[] _elements = entity.GetType().GetProperties();
                foreach (PropertyInfo _property in _elements)
                {
                    #region PartitionKey
                    if (_property.Name == PartitionKey)
                    {
                        _property.SetValue(entity, _tableEntity.PartitionKey);
                        continue;
                    }
                    #endregion

                    #region RowKey
                    if (_property.Name == RowKey)
                    {
                        _property.SetValue(entity, _tableEntity.RowKey);
                        continue;
                    }
                    #endregion

                    
                    _property.SetValue(entity, _tableEntity.Properties[_property.Name]);
                }
                #endregion

                return entity;
            }
            #endregion
            #region catch
            catch (Exception ex)
            {
                string error = "Error in function " + MethodBase.GetCurrentMethod().Name + " - " + ex.Message;
                System.Diagnostics.Trace.TraceError(error);
                return null;
            }
            #endregion
        }
    }
}
