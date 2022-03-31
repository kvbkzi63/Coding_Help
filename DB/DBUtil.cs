using Dapper;
using Dapper.Contrib.Extensions; 
using Newtonsoft.Json;
//using Newtonsoft.Json;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace DB
{
    public class DBhelp
    {
        const int BATCH_SIZE = 1024;

        /// <summary>
        /// ConnectionTimeout
        /// </summary>
        public int ConnectionTimeout { get; set; } = 20;

        /// <summary>
        /// Default DB User Account
        /// </summary>
        public string defaultDBUser { get; set; } = "";

        /// <summary>
        /// Dump Exec DB Command
        /// </summary>
        public StringBuilder dumpDBCommand { get; set; } = new StringBuilder();

        /// <summary>
        /// 日期相關參數
        /// </summary>
        public enum DateTimeCompare
        {
            [System.ComponentModel.DataAnnotations.Display(Name = "等於")]
            [Description("=")]
            EqualTo = 0,

            [System.ComponentModel.DataAnnotations.Display(Name = "大於等於")]
            [Description(">=")]
            IsMoreThanOrEqualTo = 1,

            [System.ComponentModel.DataAnnotations.Display(Name = "大於")]
            [Description(">")]
            IsMoreThano = 2,

            [System.ComponentModel.DataAnnotations.Display(Name = "小於等於")]
            [Description("<=")]
            IsLessThanOrEqualTo = 3,

            [System.ComponentModel.DataAnnotations.Display(Name = "小於")]
            [Description("<")]
            IsLessThano = 4,
        }

        /// <summary>
        /// 查詢用
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql"></param>
        /// <returns></returns>
        public IEnumerable<T> query<T>(string sql)
        {
            string constr = PUB.DecryptUtility.Instance.GetDecypConnectionString(defaultDBUser);
            try
            {
                using (OracleConnection oracleConn = new OracleConnection(constr))
                {
                    IEnumerable<T> result = oracleConn.Query<T>(sql);
                    oracleConn.Close();
                    return result;
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// 執行用(INSERT、UPDATE、DELETE)
        /// </summary>
        /// <param name="sql"></param>
        public void excute(string sql)
        {
            string constr = PUB.DecryptUtility.Instance.GetDecypConnectionString(defaultDBUser);
            try
            {
                using (OracleConnection oracleConn = new OracleConnection(constr))
                {
                    oracleConn.Execute(sql);
                    oracleConn.Close();
                }
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Execute 
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <returns>int</returns>
        public int Execute(string sql, object parameters = null)
        {
            string constr = PUB.DecryptUtility.Instance.GetDecypConnectionString(defaultDBUser);
            IDbConnection oracleConn = null;
            IDbTransaction _trans = null;
            int row = 0;
            try
            {
                using (oracleConn = new OracleConnection(constr))
                {
                    oracleConn.Open();
                    using (_trans = oracleConn.BeginTransaction())
                    {
                        row = oracleConn.Execute(sql, parameters, _trans);
                        _trans.Commit();
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                _trans.Dispose();
                oracleConn.Close();
                oracleConn.Dispose();
            }

            return row;
        }

        /// <summary>
        /// 多筆SQL執行用(INSERT、UPDATE、DELETE)
        /// </summary>
        /// <param name="sql"></param>
        public void batchExcute(List<string> sqlList)
        {
            string constr = PUB.DecryptUtility.Instance.GetDecypConnectionString(defaultDBUser);
            try
            {
                using (OracleConnection oracleConn = new OracleConnection(constr))
                {
                    oracleConn.Open();
                    IDbTransaction transaction = oracleConn.BeginTransaction();
                    try
                    {
                        foreach (string sql in sqlList)
                        {
                            oracleConn.Execute(sql, null, transaction);
                        }

                        transaction.Commit();
                    }
                    catch
                    {
                        transaction.Rollback();
                        throw;
                    }
                    oracleConn.Close();
                }
            }
            catch
            {
                throw;
            }
        }

        /// <summary>
        /// 建立資料庫連線
        /// </summary>
        /// <param name="readOnlyConnection">是否唯讀</param>
        /// <returns>資料庫連線</returns>
        private IDbConnection GetDbConnection(bool readOnlyConnection = false)
        {
            var constr = PUB.DecryptUtility.Instance.GetDecypConnectionString(defaultDBUser);
            OracleConnection dBConnection = new OracleConnection(constr);

            if (dBConnection.State != ConnectionState.Open)
            {
                dBConnection.Open();
            }

            return dBConnection;
        }

        #region Query 系列
        /// <summary>
        /// 查詢用
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql">Script</param>
        /// <param name="readOnlyConnection">唯讀參數</param>
        /// <param name="param">參數</param>
        /// <returns></returns>
        public DataTable Query(string sql, bool readOnlyConnection = false, OracleParameter[] param = null)
        {
            string constr = PUB.DecryptUtility.Instance.GetDecypConnectionString(defaultDBUser);
            DataSet dataSet = new DataSet();
            DataTable datatable = new DataTable();

            try
            {
                using (var conn = GetDbConnection(readOnlyConnection) as OracleConnection)
                {
                    OracleCommand command = new OracleCommand(sql, conn);
                    if (param != null)
                    {
                        command.Parameters.AddRange(param);
                    }
                    new OracleDataAdapter(command).Fill(dataSet);
                    datatable = dataSet.Tables[0];
                }
            }
            catch (Exception)
            {
                throw;
            }
            return datatable;
        }

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="param">查詢參數物件</param>
        /// <param name="timeoutSecs">SQL執行Timeout秒數</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>x
        public async Task<IEnumerable<TReturn>> QueryAsyncwithTimeoutTime<TReturn>(string querySql, object param = null, int timeoutSecs = 20, bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            try
            {
                using (IDbConnection con = GetDbConnection(readOnlyConnection))
                {
                    return await con.QueryAsync<TReturn>(querySql, param, null, timeoutSecs, commandType).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 查詢資料
        /// (keyEntity的name請使用nameof，若想使用like則在Value內使用%，則自動轉換為Like, 若value為陣列則自動傳換為IN, 若value為DateTime則條件自動為大於等於, 若value為!Null則條件自動為Not Null, 若value為!Y則條件自動為 <> 'Y')
        /// </summary>
        /// <typeparam name="TResult">資料封裝的物件類型</typeparam>
        /// <param name="paramDictoionary">查詢條件</param>
        /// <returns></returns>
        public async Task<IEnumerable<TResult>> QueryAsync<TResult>(Dictionary<string, object> paramDictoionary)
            where TResult : new()
        {
            DynamicParameters keyEntity = new DynamicParameters();
            foreach (var item in paramDictoionary)
            {
                keyEntity.Add(item.Key, item.Value);
            }

            return await QueryAsync<TResult>(keyEntity).ConfigureAwait(false);
        }

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TFirst">回覆的資料類型1</typeparam>
        /// <typeparam name="TSecond">回覆的資料類型2</typeparam>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="map">資料物件的組成方法</param>
        /// <param name="param">查詢參數物件</param>
        /// <param name="splitOn">使用join條件時，回傳資料的切分欄位</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(string querySql, Func<TFirst, TSecond, TReturn> map, object param = null, string splitOn = "Id", bool readOnlyConnection = false, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync(querySql, map, param: param, splitOn: splitOn, commandType: commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TFirst">回覆的資料類型1</typeparam>
        /// <typeparam name="TSecond">回覆的資料類型2</typeparam>
        /// <typeparam name="TThird">回覆的資料類型3</typeparam>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="map">資料物件的組成方法</param>
        /// <param name="param">查詢參數</param>
        /// <param name="splitOn">使用join條件時，回傳資料的切分欄位</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(string querySql, Func<TFirst, TSecond, TThird, TReturn> map, object param = null, string splitOn = "Id", bool readOnlyConnection = true, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync(querySql, map, param: param, splitOn: splitOn, commandType: commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢資料
        /// </summary>
        /// <typeparam name="TFirst">回覆的資料類型1</typeparam>
        /// <typeparam name="TSecond">回覆的資料類型2</typeparam>
        /// <typeparam name="TThird">回覆的資料類型3</typeparam>
        /// <typeparam name="TReturn">回覆的資料類型</typeparam>
        /// <param name="querySql">SQL敘述</param>
        /// <param name="map">資料物件的組成方法</param>
        /// <param name="param">查詢參數</param>
        /// <param name="splitOn">使用join條件時，回傳資料的切分欄位</param>
        /// <param name="readOnlyConnection">是否使用 Read Only Connetion</param>
        /// <param name="commandType">敘述類型</param>
        /// <returns>資料物件</returns>
        public async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFifth, TReturn>(string querySql, Func<TFirst, TSecond, TThird, TFifth, TReturn> map, object param = null, string splitOn = "Id", bool readOnlyConnection = true, CommandType commandType = CommandType.Text)
        {
            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync(querySql, map, param: param, splitOn: splitOn, commandType: commandType).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 查詢資料
        /// (keyEntity的name請使用nameof，若想使用like則在Value內使用%，則自動轉換為Like, 若value為陣列則自動傳換為IN, 若value為DateTime則條件自動為大於等於, 若value為!Null則條件自動為Not Null, 若value為!Y則條件自動為 <> 'Y')
        /// </summary>
        /// <typeparam name="TResult">資料封裝的物件類型</typeparam>
        /// <param name="keyEntity">Where的物件</param>
        /// <param name="readOnlyConnection">是否使用Read Only Connetion</param>
        /// <returns></returns>
        public async Task<IEnumerable<TResult>> QueryAsync<TResult>(DynamicParameters keyEntity = null, bool readOnlyConnection = false)
            where TResult : new()
        {
            DynamicParameters dynamicParameters = new DynamicParameters();
            List<string> keyProperties = new List<string>();
            string querySql = string.Empty;

            Type modelType = typeof(TResult);
            bool hasTableAttribute = modelType.IsDefined(typeof(TableAttribute), true);
            string tableName = hasTableAttribute ? ((TableAttribute)modelType.GetCustomAttributes(typeof(TableAttribute), true)[0]).Name : modelType.Name;
            bool hasUserAttribute = modelType.IsDefined(typeof(DBUserAttribute), true);
            string userName = hasUserAttribute ? ((DBUserAttribute)modelType.GetCustomAttributes(typeof(DBUserAttribute), true)[0]).Name : defaultDBUser;

            List<string> queryProperties = (from entityMember in new TResult().GetType().GetProperties() where !entityMember.IsDefined(typeof(WriteAttribute), true) select entityMember.Name).ToList();

            querySql += $"SELECT {string.Join($"{Environment.NewLine}, ", queryProperties)} FROM {userName}.{tableName} ";

            if (keyEntity != null && keyEntity.ParameterNames.Any())
            {
                var parametersLookup = (SqlMapper.IParameterLookup)keyEntity;

                foreach (var keyentityParameterName in keyEntity.ParameterNames)
                {
                    var pValue = parametersLookup[keyentityParameterName];
                    if (!(pValue is string) && (pValue is ValueTuple<DateTime, DateTime>))
                    {
                        keyProperties.Add($"{keyentityParameterName} Between :key_{keyentityParameterName} And :key_{keyentityParameterName}1");
                    }
                   else if (!(pValue is string) && (pValue is ValueTuple<DateTimeCompare, DateTime>))
                    {
                        var tupleValue = (ValueTuple<DateTimeCompare, DateTime>)pValue;
                        var descriptionValue = tupleValue.Item1.GetEnumDescription();
                        keyProperties.Add($"{keyentityParameterName} {descriptionValue} :key_{keyentityParameterName} ");
                    }
                    else if (!(pValue is string) && pValue is IEnumerable)
                    {
                        keyProperties.Add($"{keyentityParameterName} IN :key_{keyentityParameterName}");
                    }
                    else if (!(pValue is string) && pValue is DateTime)
                    {
                        keyProperties.Add($"{keyentityParameterName} = :key_{keyentityParameterName}");
                    }
                    else if (!(pValue is string) && pValue == null)
                    {
                        keyProperties.Add($"{keyentityParameterName} IS NULL");
                    }
                    else
                    {
                        switch (pValue)
                        {
                            case string _ when pValue.ToString().Contains("%"):
                                keyProperties.Add($"{keyentityParameterName} LIKE :key_{keyentityParameterName}");
                                break;
                            case string _ when pValue.ToString().Equals("!Null"):
                                keyProperties.Add($"{keyentityParameterName} IS Not NULL");
                                break;
                            case string _ when pValue.ToString().Contains("!"):
                                keyProperties.Add($"{keyentityParameterName} <> :key_{keyentityParameterName}");
                                pValue = pValue.ToString().Replace("!", string.Empty);
                                break;
                            default:
                                keyProperties.Add($"{keyentityParameterName} = :key_{keyentityParameterName}");
                                break;
                        }
                    }

                    if (!(pValue is string) && (pValue is ValueTuple<DateTime, DateTime>))
                    {
                        var tupleValue = (ValueTuple<DateTime, DateTime>)pValue;
                        dynamicParameters.Add("key_" + keyentityParameterName, tupleValue.Item1);
                        dynamicParameters.Add("key_" + keyentityParameterName + "1", tupleValue.Item2);
                    }
                    else if (!(pValue is string) && (pValue is ValueTuple<DateTimeCompare, DateTime>))
                    {
                        var tupleValue = (ValueTuple<DateTimeCompare, DateTime>)pValue;
                        dynamicParameters.Add("key_" + keyentityParameterName, tupleValue.Item2);
                    }
                    else
                    {
                        dynamicParameters.Add("key_" + keyentityParameterName, pValue);
                    }
                }

                querySql += $"WHERE {string.Join($"{Environment.NewLine} AND ", keyProperties)} ";
            }

            using (IDbConnection con = GetDbConnection(readOnlyConnection))
            {
                return await con.QueryAsync<TResult>(querySql, dynamicParameters, null, ConnectionTimeout, CommandType.Text).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// ExecuteScalar 查詢第一列資料 
        /// </summary>
        /// <param name="sql">SQL</param>
        /// <param name="parameters">Parameter</param>
        /// <returns></returns>
        public object ExecuteScalar(string sql, object parameters = null)
        {
            string conStr = PUB.DecryptUtility.Instance.GetDecypConnectionString(defaultDBUser);
            OracleConnection oracleConn = new OracleConnection();
            try
            {
                using (oracleConn = new OracleConnection(conStr))
                {
                    return oracleConn.ExecuteScalar(sql, parameters);
                }
            }
            catch (Exception)
            {
                throw;
            }
            finally
            {
                oracleConn.Close();
                oracleConn.Dispose();
            }
        }

        #endregion  Query 系列

        #region Insert 系列

        /// <summary>
        /// 新增單筆或多筆資料
        /// </summary>
        /// <typeparam name="T">新增資料物件類型</typeparam>
        /// <param name="insertEntity">新增物件</param>
        /// <returns>The ID(primary key) of the newly inserted record if it is identity using the defined type, otherwise null</returns>
        public async Task<long> InsertDueToLongAsync<T>(IEnumerable<T> insertEntity)
            where T : class
        {
            BindInsert<T>(insertEntity, out string sql);
            using (IDbConnection con = GetDbConnection() as IDbConnection)
            {
                return await con.ExecuteAsync(sql, insertEntity, null, ConnectionTimeout, CommandType.Text).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// 分割清單
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="items"></param>
        /// <param name="batchSize"></param>
        /// <returns></returns>
        public IEnumerable<T[]> SplitBatch<T>(IEnumerable<T> items, int batchSize)
        {
            return items.Select((item, idx) => new { item, idx })
                .GroupBy(o => o.idx / batchSize)
                .Select(o => o.Select(p => p.item).ToArray());
        }

        /// <summary>
        /// 批次執行（並不適用於Query）
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="cmd">Script 語法</param>
        /// <param name="oraParams">參數</param>
        /// <param name="props"></param>
        /// <param name="data"></param>
        private void InsertWithArrayBinding<T>(OracleCommand cmd,
            Dictionary<string, OracleParameter> oraParams,
            Dictionary<string, PropertyInfo> props,
            T[] data)
        {
            //// 測試資料時用
            //List<string> paramListString = new List<string>();
            cmd.ArrayBindCount = data.Length;
            cmd.Parameters.Clear();
            foreach (var pn in oraParams.Keys)
            {
                var p = oraParams[pn];
                p.Value = data.Select(o => props[pn].GetValue(o)).ToArray();

                //paramListString.AddRange(data.Select(o => props[pn].GetValue(o)?.ToString() ?? string.Empty).ToList());
                cmd.Parameters.Add(p);

                //Console.WriteLine("---------------------------------------組資料---------------------------------------");
                dumpDBCommand.AppendLine(JsonConvert.SerializeObject(p));
                dumpDBCommand.AppendLine(JsonConvert.SerializeObject(p.Value));
                //Console.WriteLine(JsonConvert.SerializeObject(p.Value));
                //Console.WriteLine(JsonConvert.SerializeObject(p));
                //Console.WriteLine("---------------------------------------組資料結束---------------------------------------");
            }
            //var convertScriptString = string.Join("','", paramListString);
            cmd.ExecuteNonQuery();
            dumpDBCommand.Clear();
        }

        /// <summary>
        /// 批次寫入資料庫 (CLOB欄位可用)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <returns></returns>
        public async Task<int> ImportToOra<T>(IEnumerable<T> entities)
        {
            var tableName = typeof(T).Name;
            var userAtt = ((DBUserAttribute)typeof(T).GetCustomAttribute(typeof(DBUserAttribute), true))?.Name ?? defaultDBUser;

            var props = typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.DeclaredOnly | BindingFlags.Instance);
            var propNames = props.Select(o => o.Name).ToArray();
            var propDict = props.ToDictionary(o => o.Name, o => o);
            var oraParams = props.ToDictionary(o => o.Name, o =>
            {
                OracleParameter p = new OracleParameter { ParameterName = o.Name };
                switch (o.PropertyType.ToString().Split('.').Last().TrimEnd(']'))
                {
                    case "String":
                        p.DbType = DbType.String;
                        break;
                    case "DateTime":
                        p.DbType = DbType.DateTime;
                        //p.OracleDbType = OracleDbType.Date;
                        break;
                    case "Int32":
                        p.DbType = DbType.Int32;
                        break;
                    case "Decimal":
                        p.DbType = DbType.Decimal;
                        break;
                    case "Single":
                        p.DbType = DbType.Single;
                        break;
                    case "Double":
                        p.DbType = DbType.Double;
                        break;
                    default:
                        throw new NotImplementedException(o.PropertyType.ToString());
                }
                return p;
            });
            string insertSql = $"Insert Into {userAtt}.{tableName} ({string.Join(",", propNames)}) Values ({string.Join(",", propNames.Select(o => $":{o}").ToArray())})";
            var count = 0;
            var sw = new Stopwatch();

            using (var conn = GetDbConnection(false) as OracleConnection)//var cn = new OracleConnection(CnStr)
            {
                var cmd = conn.CreateCommand();
                cmd.BindByName = true;
                cmd.CommandTimeout = ConnectionTimeout;
                cmd.CommandText = insertSql;
                OracleTransaction trans = conn.BeginTransaction();
                cmd.Transaction = trans;
                try
                {
                    sw.Start();
                    Console.WriteLine($"開始寫入資料 - {tableName}...{DateTime.Now.ToString("yyyy/MM/dd hh:mm:ss fff")}");

                    foreach (var batchData in SplitBatch<T>(entities, BATCH_SIZE))
                    {
                        //Console.WriteLine(JsonConvert.SerializeObject(batchData));
                        count += batchData.Length;
                        var num = count - batchData.Length < 0 ? 0 : count - batchData.Length;
                        dumpDBCommand.AppendLine($"第{num}筆開始，寫入{batchData.Length}筆");
                        InsertWithArrayBinding<T>(cmd, oraParams, propDict, batchData);
                        Console.Write($"\r{count}/{entities.Count()}({count * 1.0 / entities.Count():p0})");
                    }
                    trans.Commit();
                    Console.WriteLine($"資料寫入完成 - {sw.ElapsedMilliseconds:n0}ms");
                }
                catch (Exception ex)
                {
                    trans.Rollback();
                    throw ex;
                }
                finally
                {
                    sw.Stop();
                }
            }
            return count;
        }

        /// <summary>
        /// 新增多筆資料 (CLOB欄位不可用)
        /// </summary>
        /// <typeparam name="T">資料物件類別</typeparam>
        /// <param name="entities">資料物件集合</param>
        /// <returns>新增資料筆數</returns>
        public async Task<int> BulkInsertAsync<T>(IEnumerable<T> entities)
        where T : class
        {
            int insertedRows = -1;
            int numb = entities.Count();
            if (entities.Count() >= 200)
            {
                var result = await ImportToOra(entities).ConfigureAwait(false);
                return result;
            }

            using (IDbConnection conn = GetDbConnection())
            {
                using (IDbTransaction transaction = conn.BeginTransaction(IsolationLevel.Serializable))
                {
                    try
                    {
                        BindInsert(entities, out string sql);

                        insertedRows = await conn.ExecuteAsync(sql, entities, transaction, ConnectionTimeout).ConfigureAwait(false);

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }
            return insertedRows;
        }

        /// <summary>
        /// 新增多筆資料 (CLOB欄位可用)
        /// </summary>
        /// <typeparam name="T">資料物件類別</typeparam>
        /// <param name="entities">資料物件集合</param>
        /// <returns>新增資料筆數</returns>
        public async Task<int> BulkInsertAsyncCLOB<T>(IEnumerable<T> entities)
        where T : class
        {
            int numb = entities.Count();
            if (numb >= 200)
            {
                var result = await ImportToOra(entities).ConfigureAwait(false);
                return result;
            }

            using (var con = GetDbConnection(false) as OracleConnection)//var cn = new OracleConnection(CnStr)
            //using (IDbConnection con = GetDbConnection())
            {
                using (var transaction = con.BeginTransaction())// IsolationLevel.Serializable))
                {
                    try
                    {
                        for (int i = 0; i < numb; i++)
                        {
                            BindInsert(entities.Skip(i), out string sql, out DynamicParameters param, i);
                            con.Execute(sql, param, transaction, ConnectionTimeout);
                        }

                        transaction.Commit();
                        return numb;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }
        }
        #endregion Insert 系列

        #region Update 系列
        /// <summary>
        /// 更新單筆資料(可自訂更新欄位與更新條件)
        /// </summary>
        /// <typeparam name="TDalModel">更新的 Dal Model Type</typeparam>
        /// <param name="updateInfo">更新的欄位名稱(kay)與資料(value)</param>
        /// <param name="conditionInfo">更新的欄位名稱(kay)與條件(value)</param>
        /// <returns></returns>
        public async Task<bool> UpdateAsync<TDalModel>(Dictionary<string, object> updateInfo, Dictionary<string, object> conditionInfo)
        {
            BindUpdate<TDalModel>(updateInfo, conditionInfo, out string sql, out DynamicParameters dynamicParameters);

            using (IDbConnection con = GetDbConnection())
            {
                return await con.ExecuteAsync(sql, dynamicParameters, null, ConnectionTimeout, CommandType.Text).ConfigureAwait(false) > 0;
            }
        }

        /// <summary>
        /// 更新單筆資料
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="updateRow"></param>
        /// <param name="conditionInfo"></param>
        /// <returns></returns>
        public async Task<bool> UpdateAsync<T>(T updateRow, Dictionary<string, object> conditionInfo)
        {
            BindUpdate<T>(updateRow, conditionInfo, out string sql, out DynamicParameters dynamicParameters);

            using (IDbConnection con = GetDbConnection())
            {
                return await con.ExecuteAsync(sql, dynamicParameters, null, ConnectionTimeout, CommandType.Text).ConfigureAwait(false) > 0;
            }
        }

        /// <summary>
        /// 更新整批資料
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <param name="conditionInfo"></param>
        /// <returns></returns>
        public async Task<bool> UpdateAsync<T>(IEnumerable<T> entities, Dictionary<string, object> conditionInfo)
        {
            bool result = true;
            using (IDbConnection con = GetDbConnection())
            {
                using (var transaction = con.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    try
                    {
                        foreach (var row in entities)
                        {
                            BindUpdate<T>(row, conditionInfo, out string sql, out DynamicParameters dynamicParameters);

                            var exeResult = await con.ExecuteAsync(sql, dynamicParameters, transaction, ConnectionTimeout, CommandType.Text).ConfigureAwait(false) > 0;

                            if (exeResult == false)
                            {
                                result = false;
                                break;
                            }
                        }

                        if (result)
                        {
                            transaction.Commit();
                        }
                    }
                    catch (Exception ex)
                    {
                        result = false;
                        transaction.Rollback();
                        throw;
                    }
                }
            }

            return result;
        }
        #endregion Update 系列

        #region Bind 系列
        /// <summary>
        /// 新增資料（CLOB欄位不能用），建議選用有Parameter的Function
        /// </summary>
        /// <typeparam name="T">新增的Model</typeparam>
        /// <param name="insertInfo">新增的資料</param>
        /// <param name="sql">Bind完的SQL Script</param>
        public void BindInsert<T>(IEnumerable<T> insertInfo, out string sql)
        {
            IEnumerable<string> fields = typeof(T).GetProperties()
                .Where(p =>
                    p.CustomAttributes.All(a => a.AttributeType != typeof(KeyAttribute)
                        && a.AttributeType != typeof(ComputedAttribute)
                        && a.AttributeType != typeof(NoWrite)))
                .Select(p => p.Name); // 資料實體中的所有屬性(欄位)名稱、除了標有自訂屬性的欄位外

            var tableAtt = (TableAttribute)typeof(T).GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault();
            var userAtt = ((DBUserAttribute)typeof(T).GetCustomAttribute(typeof(DBUserAttribute), true))?.Name ?? defaultDBUser;

            // default table name
            string tableName = "xxxx";
            if (tableAtt != null)
            {
                // 資料實體對應的資料表名稱;
                tableName = tableAtt.Name;
            }
            else
            {
                // class name
                tableName = typeof(T).Name;
            }

            string fieldNames = string.Join(", ", fields);
            string fieldParameters = string.Join(", :", fields);
            sql = $"INSERT INTO {userAtt}.{tableName}({fieldNames}) values(:{fieldParameters})";
        }

        /// <summary>
        /// 新增單筆資料（CLOB欄位不可這組），最好選用有Parameter這組，目前試大量資料的話，一次建議只丟1000筆
        /// </summary>
        /// <typeparam name="T">新增的Model</typeparam>
        /// <param name="insertInfo">新增的資料</param>
        /// <param name="sql">Bind完的SQL Script</param>
        /// <param name="param">Bind完的參數</param>
        /// <param name="count">第幾筆</param>
        public void BindInsertNew<T>(IEnumerable<T> insertInfo, out string sql, out DynamicParameters param, int count = 0)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();

            List<string> columns = new List<string>();
            List<string> values = new List<string>();

            var tableAtt = (TableAttribute)typeof(T).GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault();
            var userAtt = ((DBUserAttribute)typeof(T).GetCustomAttribute(typeof(DBUserAttribute), true))?.Name ?? defaultDBUser;

            // default table name
            string tableName = "xxxx";
            if (tableAtt != null)
            {
                // 資料實體對應的資料表名稱;
                tableName = tableAtt.Name;
            }
            else
            {
                // class name
                tableName = typeof(T).Name;
            }

            var props = typeof(T)
             .GetProperties(BindingFlags.Public | BindingFlags.DeclaredOnly | BindingFlags.Instance);
            var propNames = props.Select(o => o.Name).ToArray();
            var oraParams = props.ToDictionary(o => o.Name, o =>
            {
                var p = new OracleParameter();
                p.ParameterName = $"{o.Name}_{count}";
                switch (o.PropertyType.ToString().Split('.').Last().TrimEnd(']'))
                {
                    case "String":
                        p.DbType = DbType.String;
                        break;
                    case "DateTime":
                        p.DbType = DbType.DateTime;
                        break;
                    case "Int32":
                        p.DbType = DbType.Int32;
                        break;
                    case "Decimal":
                        p.DbType = DbType.Decimal;
                        break;
                    case "Single":
                        p.DbType = DbType.Single;
                        break;
                    case "Double":
                        p.DbType = DbType.Double;
                        break;
                    default:
                        throw new NotImplementedException(o.PropertyType.ToString());
                }
                return p;
            });

            foreach (var property in props)
            {
                // 檢查是否有 KeyAttribute 或 WriteAttribute 或 NoWrite 定義，有則排除，不組進SQL裡
                if (!property.IsDefined(typeof(KeyAttribute), true) &&
                    !property.IsDefined(typeof(WriteAttribute), true) &&
                    !property.IsDefined(typeof(NoWrite), true))
                {
                    columns.Add($"{property.Name}");
                    values.Add($":{property.Name}_{count}");

                    oraParams.TryGetValue(property.Name, out var oraParam);

                    switch (oraParam.DbType)
                    {
                        case DbType.Date:
                        case DbType.DateTime:
                        case DbType.DateTime2:
                        case DbType.DateTimeOffset:
                            dynamicParameters.Add($":{property.Name}_{count}", insertInfo.Select(x => ((DateTime?)property.GetValue(x))?.ToString("yyyy-MM-dd hh:mm:ss")).ToArray(), oraParam.DbType, ParameterDirection.Input);
                            //dynamicParameters.Add($":{property.Name}_{count}", insertInfo.Select(x => $"to_date({property.GetValue(x)}, "'yyyy-MM-dd HH24:MI:SS'")").ToArray(), oraParam.DbType,ParameterDirection.Input);
                            break;
                        default:
                            dynamicParameters.Add($":{property.Name}_{count}", insertInfo.Select(x => property.GetValue(x)).ToArray(), oraParam.DbType, ParameterDirection.Input);
                            break;
                    }
                }
            }

            sql = $"INSERT INTO {userAtt}.{tableName} ({string.Join(", ", columns)}) values({string.Join(", ", values)}) ";
            param = dynamicParameters;
        }

        /// <summary>
        /// 新增單筆資料（CLOB欄位應用這組）(請用迴圈取)
        /// </summary>
        /// <typeparam name="T">新增的Model</typeparam>
        /// <param name="insertInfo">新增的資料</param>
        /// <param name="sql">Bind完的SQL Script</param>
        /// <param name="param">Bind完的參數</param>
        /// <param name="count">第幾筆</param>
        public void BindInsert<T>(IEnumerable<T> insertInfo, out string sql, out DynamicParameters param, int count = 0)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();

            List<string> columns = new List<string>();
            List<string> values = new List<string>();

            var tableAtt = (TableAttribute)typeof(T).GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault();
            var userAtt = ((DBUserAttribute)typeof(T).GetCustomAttribute(typeof(DBUserAttribute), true))?.Name ?? defaultDBUser;

            // default table name
            string tableName = "xxxx";
            if (tableAtt != null)
            {
                // 資料實體對應的資料表名稱;
                tableName = tableAtt.Name;
            }
            else
            {
                // class name
                tableName = typeof(T).Name;
            }

            string no = count == 0 ? string.Empty : count.ToString();
            var props = typeof(T)
             .GetProperties(BindingFlags.Public | BindingFlags.DeclaredOnly | BindingFlags.Instance);
            var propNames = props.Select(o => o.Name).ToArray();

            foreach (var property in props)
            {
                // 檢查是否有 KeyAttribute 或 WriteAttribute 定義，有則排除，不組進SQL裡
                if (!property.IsDefined(typeof(KeyAttribute), true) &&
                    !property.IsDefined(typeof(WriteAttribute), true) &&
                    !property.IsDefined(typeof(NoWrite), true))
                {
                    columns.Add($"{property.Name}");
                    values.Add($":{property.Name}{no}");
                    //dynamicParameters.Add($"{property.Name}{no}", insertInfo.Select(o => property.GetValue(o)).FirstOrDefault(), p.DbType);
                    dynamicParameters.Add($":{property.Name}{no}", property.GetValue(insertInfo.FirstOrDefault()));
                }
            }

            sql = $"INSERT INTO {userAtt}.{tableName} ({string.Join(", ", columns)}) values({string.Join(", ", values)}) ";
            param = dynamicParameters;
        }

        /// <summary>
        /// 更新資料(可自訂更新欄位與更新條件)
        /// </summary>
        /// <typeparam name="TDalModel">更新的 Dal Model Type</typeparam>
        /// <param name="updateInfo">更新的欄位名稱(kay)與資料(value)</param>
        /// <param name="conditionInfo">更新的欄位名稱(kay)與條件(value)</param>
        /// <param name="sql">Bind完的SQL Script</param>
        /// <param name="parameters">Bind完的參數</param>
        /// <returns></returns>
        public void BindUpdate<TDalModel>(Dictionary<string, object> updateInfo, Dictionary<string, object> conditionInfo, out string sql, out DynamicParameters parameters)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();
            List<string> updateProperties = new List<string>();
            List<string> conditionProperties = new List<string>();

            var modelType = typeof(TDalModel);
            var hasTableAttribute = modelType.IsDefined(typeof(TableAttribute), true);
            var tableName = hasTableAttribute ? ((TableAttribute)modelType.GetCustomAttributes(typeof(TableAttribute), true)[0]).Name : modelType.Name;

            bool hasUserAttribute = modelType.IsDefined(typeof(DBUserAttribute), true);
            string userName = hasUserAttribute ? ((DBUserAttribute)modelType.GetCustomAttributes(typeof(DBUserAttribute), true)[0]).Name : defaultDBUser;

            string SqlStrUpdate(KeyValuePair<string, object> item, string alias)
            {
                dynamicParameters.Add($"{alias}_{item.Key}", item.Value);
                return $"{item.Key} = :{alias}_{item.Key}";
            }

            string SqlStrWhere(KeyValuePair<string, object> item, string alias)
            {
                var sql = string.Empty;

                if (!(item.Value is string) && (item.Value is ValueTuple<DateTime, DateTime>))
                {
                    sql = $"{item.Key} Between :{alias}_{item.Key} And :{alias}_{item.Key}1";
                }
                else if (!(item.Value is string) && (item.Value is ValueTuple<DateTimeCompare, DateTime>))
                {
                    var tupleValue = (ValueTuple<DateTimeCompare, DateTime>)item.Value;
                    var descriptionValue = tupleValue.Item1.GetEnumDescription();
                    sql = $"{item.Key} {descriptionValue} :{alias}_{item.Key} ";
                }
                else if (!(item.Value is string) && item.Value is IEnumerable)
                {
                    sql = $"{item.Key} IN :{alias}_{item.Key}";
                }
                else
                {
                    if (item.Value is string && item.Value.ToString().Contains("%"))
                    {
                        sql = $"{item.Key} LIKE :{alias}_{item.Key}";
                    }
                    else if (item.Value == null)
                    {
                        sql = $"{item.Key} IS NULL";
                    }
                    else
                    {
                        sql = $"{item.Key} = :{alias}_{item.Key}";
                    }
                }

                if (!(item.Value is string) && (item.Value is ValueTuple<DateTime, DateTime>))
                {
                    var tupleValue = (ValueTuple<DateTime, DateTime>)item.Value;
                    dynamicParameters.Add($"{alias}_{item.Key}", tupleValue.Item1);
                    dynamicParameters.Add($"{alias}_{item.Key}1", tupleValue.Item2);
                }
                else if (!(item.Value is string) && (item.Value is ValueTuple<DateTimeCompare, DateTime>))
                {
                    var tupleValue = (ValueTuple<DateTimeCompare, DateTime>)item.Value;
                    dynamicParameters.Add($"{alias}_{item.Key}", tupleValue.Item2);
                }
                else
                {
                    dynamicParameters.Add($"{alias}_{item.Key}", item.Value);
                }

                return sql;
            }

            updateProperties = updateInfo.Select(s => SqlStrUpdate(s, "set")).ToList();
            conditionProperties = conditionInfo.Select(s => SqlStrWhere(s, "key")).ToList();

            sql = $"Update {userName}.{tableName} Set {Environment.NewLine} {string.Join($"{Environment.NewLine}, ", updateProperties)} {Environment.NewLine} Where {string.Join($"{Environment.NewLine} And ", conditionProperties)}";
            parameters = dynamicParameters;
        }

        /// <summary>
        /// 更新資料(可自訂更新條件)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="updateValue"></param>
        /// <param name="conditionInfo"></param>
        /// <param name="sql"></param>
        /// <param name="parameter"></param>
        public void BindUpdate<T>(T updateValue, Dictionary<string, object> conditionInfo, out string sql, out DynamicParameters parameter)
        {
            Dictionary<string, object> updateInfo = new Dictionary<string, object>();

            var props = typeof(T)
             .GetProperties(BindingFlags.Public | BindingFlags.DeclaredOnly | BindingFlags.Instance);

            foreach (var property in props)
            {
                // 檢查是否有 KeyAttribute 或 WriteAttribute 定義，有則排除，不組進SQL裡
                if (!property.IsDefined(typeof(KeyAttribute), true) &&
                    !property.IsDefined(typeof(WriteAttribute), true))
                {
                    updateInfo.Add(property.Name, property.GetValue(updateValue));
                }
            }

            // 移除 Key的更新
            if (conditionInfo.Any())
            {
                foreach (var rowCondition in conditionInfo)
                {
                    if (updateInfo.TryGetValue(rowCondition.Key, out object value))
                    {
                        updateInfo.Remove(rowCondition.Key);
                    }
                }
            }

            this.BindUpdate<T>(updateInfo, conditionInfo, out sql, out parameter);
        }

        /// <summary>
        /// 刪除資料(可自訂刪除條件)
        /// </summary>
        /// <typeparam name="TDalModel">更新的 Dal Model Type</typeparam>
        /// <param name="conditionInfo">更新的欄位名稱(kay)與條件(value)</param>
        /// <param name="sql">Bind完的SQL Script</param>
        /// <param name="parameters">Bind完的參數</param>
        /// <returns></returns>
        public void BindDelete<TDalModel>(Dictionary<string, object> conditionInfo, out string sql, out DynamicParameters parameters)
        {
            DynamicParameters dynamicParameters = new DynamicParameters();
            List<string> conditionProperties = new List<string>();

            var modelType = typeof(TDalModel);
            var hasTableAttribute = modelType.IsDefined(typeof(TableAttribute), true);
            var tableName = hasTableAttribute ? ((TableAttribute)modelType.GetCustomAttributes(typeof(TableAttribute), true)[0]).Name : modelType.Name;

            bool hasUserAttribute = modelType.IsDefined(typeof(DBUserAttribute), true);
            string userName = hasUserAttribute ? ((DBUserAttribute)modelType.GetCustomAttributes(typeof(DBUserAttribute), true)[0]).Name : defaultDBUser;

            string SqlStrWhere(KeyValuePair<string, object> item, string alias)
            {
                var sql = string.Empty;

                if (!(item.Value is string) && (item.Value is ValueTuple<DateTime, DateTime>))
                {
                    sql = $"{item.Key} Between :{alias}_{item.Key} And :{alias}_{item.Key}1";
                }
                else if (!(item.Value is string) && (item.Value is ValueTuple<DateTimeCompare, DateTime>))
                {
                    var tupleValue = (ValueTuple<DateTimeCompare, DateTime>)item.Value;
                    var descriptionValue = tupleValue.Item1.GetEnumDescription();
                    sql = $"{item.Key} {descriptionValue} :{alias}_{item.Key} ";
                }
                else if (!(item.Value is string) && item.Value is IEnumerable)
                {
                    sql = $"{item.Key} IN :{alias}_{item.Key}";
                }
                else
                {
                    if (item.Value is string && item.Value.ToString().Contains("%"))
                    {
                        sql = $"{item.Key} LIKE :{alias}_{item.Key}";
                    }
                    else if (item.Value == null)
                    {
                        sql= $"{item.Key} IS NULL";
                    }
                    else
                    {
                        sql = $"{item.Key} = :{alias}_{item.Key}";
                    }
                }

                if (!(item.Value is string) && (item.Value is ValueTuple<DateTime, DateTime>))
                {
                    var tupleValue = (ValueTuple<DateTime, DateTime>)item.Value;
                    dynamicParameters.Add($"{alias}_{item.Key}", tupleValue.Item1);
                    dynamicParameters.Add($"{alias}_{item.Key}1", tupleValue.Item2);
                }
                else if (!(item.Value is string) && (item.Value is ValueTuple<DateTimeCompare, DateTime>))
                {
                    var tupleValue = (ValueTuple<DateTimeCompare, DateTime>)item.Value;
                    dynamicParameters.Add($"{alias}_{item.Key}", tupleValue.Item2);
                }
                else
                {
                    dynamicParameters.Add($"{alias}_{item.Key}", item.Value);
                }

                return sql;
            }

            conditionProperties = conditionInfo.Select(s => SqlStrWhere(s, "key")).ToList();

            sql = $"Delete {userName}.{tableName} Where {string.Join($"{Environment.NewLine} And ", conditionProperties)}";

            if (conditionProperties.Any() == false)
            {
                sql = sql.Replace(" Where ", string.Empty);
            }

            parameters = dynamicParameters;
        }
        #endregion Bind 系列

        #region Execute 系列
        /// <summary>
        /// 執行交易 query
        /// </summary>
        /// <param name="taskList">任務清單</param>
        /// <returns></returns>
        public bool ExecuteTransactionQuery(params Action<OracleConnection, OracleTransaction>[] taskList)
        //public bool ExecuteTransactionQuery(params Action<IDbConnection, IDbTransaction>[] taskList)
        {
            using (OracleConnection con = GetDbConnection(false) as OracleConnection)//var cn = new OracleConnection(CnStr)
            //using (IDbConnection con = GetDbConnection())
            {
                using (OracleTransaction transaction = con.BeginTransaction())// IsolationLevel.Serializable))
                {
                    try
                    {
                        foreach (Action<OracleConnection, OracleTransaction> act in taskList)
                        {
                            act(con, transaction);
                        }

                        transaction.Commit();
                        return true;
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw;
                    }
                }
            }
        }
        #endregion Execute 系列

        #region GetClass  
        /// <summary>
        /// Dictionary To Class
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dict"></param>
        /// <returns></returns>
        public T GetClass<T>(Dictionary<string, object> dict)
        {
            Type type = typeof(T);
            var obj = Activator.CreateInstance(type);
            foreach (var kv in dict)
            {
                switch (type.GetProperty(kv.Key).PropertyType.ToString().Split('.').Last().TrimEnd(']'))
                {
                    case "String":
                        type.GetProperty(kv.Key).SetValue(obj, kv.Value.ToString().Trim());
                        break;
                    default:
                        type.GetProperty(kv.Key).SetValue(obj, kv.Value);
                        break;
                }
            }
            return (T)obj;
        }
        #endregion GetClass
    }
}
