using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text;
using System.Xml;
using System.Xml.Serialization;

namespace Help
{
    /// <summary>
    /// Provide JSON/XML convertor
    /// </summary>
    public class ConvertExtensions
    {
        /// <summary>
        /// 反序列化JSON字串
        /// </summary>
        /// <typeparam name="T">反序列化的類別</typeparam>
        /// <param name="json">欲反序列化的json字串</param>
        /// <returns>回傳物件</returns>
        public static T JsonDeserialize<T>(string json)
        {
            return JsonConvert.DeserializeObject<T>(json);
        }

        /// <summary>
        /// 序列化物件成JSON字串
        /// </summary>
        /// <param name="source">欲序列化的物件</param>
        /// <returns>回傳JSON字串</returns>
        public static string JsonSerialize(object source)
        {
            return JsonConvert.SerializeObject(source);
        }

        /// <summary>
        /// 反序列化XML文字成物件(<inheritdoc/>)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="xml"></param>
        /// <param name="rootAttributeName"></param>
        /// <returns></returns>
        public virtual T XmlDeserialize<T>(string xml, string rootAttributeName = null)
            where T : class
        {
            var xmlSerializer = rootAttributeName == null ? new XmlSerializer(typeof(T)) : new XmlSerializer(typeof(T), new XmlRootAttribute(rootAttributeName));

            T resultObject;
            using (var sr = new StringReader(xml))
            {
                using (var reader = XmlReader.Create(sr))
                {
                    resultObject = (T)xmlSerializer.Deserialize(reader);
                }
            }

            return resultObject;
        }

        /// <summary>
        /// 序列化物件成XML文字(XML資料不多可以用)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">資料</param>
        /// <param name="encodeType">Encoding</param>
        /// <param name="indent">是否縮排</param>
        /// <returns></returns>
        public virtual string XmlSerialize<T>(object source, Encoding encodeType = null, bool indent = false)
        {
            var xmlSerializer = new XmlSerializer(typeof(T));

            if (encodeType == null)
            {
                // Default Big5
                // Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                encodeType = Encoding.GetEncoding(950);//CodePagesEncodingProvider.Instance.GetEncoding(950);
            }

            string xml;

            using (var sw = new MemoryStream())
            {
                var settings = new XmlWriterSettings { Encoding = encodeType, Indent = indent };

                using (var writer = XmlWriter.Create(sw, settings))
                {
                    var xnameSpace = new XmlSerializerNamespaces();
                    xnameSpace.Add(string.Empty, string.Empty);
                    xmlSerializer.Serialize(writer, source, xnameSpace);
                    xml = encodeType.GetString(sw.ToArray());
                }
            }

            return xml;
        }

        /// <summary>
        /// 序列化物件成XML文字(XML資料太多時，直接存檔，避免記憶體爆掉)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">資料</param>
        /// <param name="encodeType">Encoding</param>
        /// <param name="indent">是否縮排</param>
        /// <param name="filePath">存檔路徑</param>
        /// <returns></returns>
        public void CreateXML<T>(object source, string savePath, Encoding encodeType, bool indent = false)
        {
            var xmlSerializer = new XmlSerializer(typeof(T));

            if (encodeType == null)
            {
                // Default Big5
                // Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
                encodeType = Encoding.GetEncoding(950);//CodePagesEncodingProvider.Instance.GetEncoding(950);
            }

            string xml = string.Empty;

            using (var sw = new MemoryStream())
            {
                var settings = new XmlWriterSettings { Encoding = encodeType, Indent = indent };

                using (var writer = XmlWriter.Create(sw, settings))
                {
                    var xnameSpace = new XmlSerializerNamespaces();
                    xnameSpace.Add(string.Empty, string.Empty);
                    xmlSerializer.Serialize(writer, source, xnameSpace);
                }
                var swArray = sw.ToArray();
                File.WriteAllBytes(savePath, swArray);
            }
        }

        /// <inheritdoc/>
        public string FileConvertToBase64(string filePath)
        {
            byte[] fileBytes = File.ReadAllBytes(filePath);
            string fileBase64 = Convert.ToBase64String(fileBytes);

            return fileBase64;
        }

        /// <summary>
        /// IEnumerable To DataTable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public DataTable LinqQueryToDataTable<T>(IEnumerable<T> query)
        {
            DataTable tbl = new DataTable();
            PropertyInfo[] props = null;
            foreach (T item in query)
            {
                if (props == null) //尚未初始化
                {
                    Type t = item.GetType();
                    props = t.GetProperties();
                    foreach (PropertyInfo pi in props)
                    {
                        Type colType = pi.PropertyType;
                        //針對Nullable<>特別處理
                        if (colType.IsGenericType && colType.GetGenericTypeDefinition() == typeof(Nullable<>))
                        {
                            colType = colType.GetGenericArguments()[0];
                        }
                        //建立欄位
                        tbl.Columns.Add(pi.Name, colType);
                    }
                }
                DataRow row = tbl.NewRow();
                foreach (PropertyInfo pi in props)
                {
                    row[pi.Name] = pi.GetValue(item, null) ?? DBNull.Value;
                }
                tbl.Rows.Add(row);
            }
            return tbl;
        }

        /// <summary>
        /// Model To DataTable
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <returns></returns>
        public DataTable LinqQueryToDataTable<T>(T query)
        {
            DataTable tbl = new DataTable();
            PropertyInfo[] props = null;
            if (props == null) //尚未初始化
            {
                Type t = query.GetType();
                props = t.GetProperties();
                foreach (PropertyInfo pi in props)
                {
                    Type colType = pi.PropertyType;
                    //針對Nullable<>特別處理
                    if (colType.IsGenericType && colType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        colType = colType.GetGenericArguments()[0];
                    }
                    //建立欄位
                    tbl.Columns.Add(pi.Name, colType);
                }
            }
            DataRow row = tbl.NewRow();
            foreach (PropertyInfo pi in props)
            {
                row[pi.Name] = pi.GetValue(query, null) ?? DBNull.Value;
            }
            tbl.Rows.Add(row);
            return tbl;
        }

    }
}