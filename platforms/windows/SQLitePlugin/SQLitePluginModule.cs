/*
* SQLitePluginModule.cs
* 
* A React Native module that wraps SQLite.
* 
* Thread-safety. Except where otherwise noted, all of this class's code runs on a single
* ActionQueue which frees us from having to worry about thread-safety. We have a programming
* model similar to that of the UI thread.
* 
* All ReactMethods must run on the same AwaitingQueue. This prevents the ReactMethods from
* interleaving when an `await` happens. This design enables us to think of each ReactMethod
* as being atomic relative to the other ReactMethods.
*/

using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ReactNative.Managed;
using System.Diagnostics;
using Microsoft.ReactNative;

namespace SQLitePlugin
{
    /// <summary>
    /// A module that allows JS to utilize sqlite databases.
    /// </summary>

    [ReactModule("SQLite")]
    internal sealed class SQLitePlugin
    {
        public enum WebSQLError
        {
            Unknown = 0,
            Database = 1,
            Version = 2,
            TooLarge = 3,
            Quota = 4,
            Syntax = 5,
            Constraint = 6,
            Timeout = 7
        }

        private static readonly IntPtr NegativePointer = new IntPtr(-1);

        private static WebSQLError sqliteToWebSQLError(SQLite.Net.Interop.Result sqliteError)
        {
            switch (sqliteError)
            {
                case SQLite.Net.Interop.Result.Error:
                    return WebSQLError.Syntax;
                case SQLite.Net.Interop.Result.Full:
                    return WebSQLError.Quota;
                case SQLite.Net.Interop.Result.Constraint:
                    return WebSQLError.Constraint;
                default:
                    return WebSQLError.Unknown;
            }
        }

        public class SQLiteError
        {
            public WebSQLError code { get; private set; }
            public string message { get; private set; }

            public SQLiteError(WebSQLError aCode, string aMessage)
            {
                code = aCode;
                message = aMessage;
            }
        }

        private class RNSQLiteException : Exception
        {
            public object JsonMessage { get; private set; }

            public RNSQLiteException() : base()
            {
            }

            public RNSQLiteException(object jsonMessage) : base()
            {
                JsonMessage = jsonMessage;
            }

            public RNSQLiteException(string message) : base(message)
            {
                JsonMessage = message;
            }

            public RNSQLiteException(string message, Exception inner) : base(message, inner)
            {
                JsonMessage = message;
            }
        }

        private static byte[] GetNullTerminatedUtf8(string s)
        {
            var utf8Length = Encoding.UTF8.GetByteCount(s);
            var bytes = new byte[utf8Length + 1];
            Encoding.UTF8.GetBytes(s, 0, s.Length, bytes, 0);
            return bytes;
        }

        // Throws when the file already exists.
        private static Windows.Foundation.IAsyncOperation<Windows.Storage.StorageFile> CopyDbAsync(Windows.Storage.StorageFile srcDbFile, string destDbFileName)
        {
            // This implementation is closely related to ResolveDbFilePath.
            return srcDbFile.CopyAsync(Windows.Storage.ApplicationData.Current.LocalFolder, destDbFileName, Windows.Storage.NameCollisionOption.FailIfExists);
        }

        private static string ResolveDbFilePath(string dbFileName)
        {
            // This implementation is closely related to CopyDbAsync.
            return Path.Combine(Windows.Storage.ApplicationData.Current.LocalFolder.Path, dbFileName);
        }

        private static Windows.Foundation.IAsyncOperation<Windows.Storage.StorageFile> ResolveAssetFile(string assetFilePath, string dbFileName)
        {
            if (assetFilePath == null || assetFilePath.Length == 0)
            {
                return null;
            }
            else if (assetFilePath == "1")
            {
                // Built path to pre-populated DB asset from app bundle www subdirectory
                return Windows.Storage.StorageFile.GetFileFromApplicationUriAsync(new Uri(
                    "ms-appx:///www/" + dbFileName));
            }
            else if (assetFilePath[0] == '~')
            {
                // Built path to pre-populated DB asset from app bundle subdirectory
                return Windows.Storage.StorageFile.GetFileFromApplicationUriAsync(new Uri(
                    "ms-appx:///" + assetFilePath.Substring(1)));
            }
            else
            {
                // Built path to pre-populated DB asset from app sandbox directory
                return Windows.Storage.StorageFile.GetFileFromApplicationUriAsync(new Uri(
                    "ms-appdata:///local/" + assetFilePath));
            }
        }

        private class OpenDB
        {
            public SQLite.Net.Interop.IDbHandle Handle { get; private set; }
            public string Path { get; private set; }

            public OpenDB(SQLite.Net.Interop.IDbHandle handle, string path)
            {
                Handle = handle;
                Path = path;
            }
        }

        private readonly SQLite.Net.Platform.WinRT.SQLiteApiWinRT _sqliteAPI;
        private readonly Dictionary<string, OpenDB> _openDBs = new Dictionary<string, OpenDB>();
        private readonly SQLite.Net.AwaitingQueue _awaitingQueue = new SQLite.Net.AwaitingQueue();
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        /// <summary>
        /// Instantiates the <see cref="SQLitePluginModule"/>.
        /// </summary>
        public SQLitePlugin() : base()
        {
            _sqliteAPI = new SQLite.Net.Platform.WinRT.SQLiteApiWinRT(tempFolderPath: null, useWinSqlite: false);
        }


        /*public override void OnReactInstanceDispose()
        {
            _cancellationTokenSource.Cancel();
            // TODO: When React Native Windows introduces asynchronous disposal
            //   (`OnReactInstanceDisposeAsync`), we should start using that and
            //   we should run this work on the awaiting queue. Currently, there's
            //   a race condition where we might close the database while one of
            //   the ReactMethods is in the middle of an `await`.
            //   https://github.com/Microsoft/react-native-windows/issues/1873
            foreach (var dbInfoPair in _openDBs)
            {
                if (_sqliteAPI.Close(dbInfoPair.Value.Handle) != SQLite.Net.Interop.Result.OK)
                {
                    System.Diagnostics.Debug.WriteLine("SQLitePluginModule: Error closing database: " + dbInfoPair.Value.Path);
                }
            }
            _openDBs.Clear();
        }*/

        private async void QueueWithCancellation(Func<Task> work)
        {
            await _awaitingQueue.RunOrQueue(work, _cancellationTokenSource.Token);
        }

        public class EchoStringValueOptions
        {
            public string value { get; set; }
        }

        [ReactMethod]
        public void echoStringValue(EchoStringValueOptions options, Action<string> success, Action<string> error)
        {
            QueueWithCancellation(() =>
            {
                success.Invoke(options.value);
                return Task.CompletedTask;
            });
        }

        public class OpenOptions
        {
            // Path at which to store the database
            public string name { get; set; }

            // Optional. When creating the DB, uses this file as the initial state.
            public string AssetFileName { get; set; }

            public bool ReadOnly { get; set; }
        }

        [ReactMethod]
        public void open(OpenOptions options, Action<string> success, Action<string> error)
        {
            QueueWithCancellation(async () =>
            {
                var dbFileName = options.name;

                if (dbFileName == null)
                {
                    error.Invoke("You must specify database name");
                    return;
                }

                if (_openDBs.ContainsKey(dbFileName))
                {
                    success.Invoke("Database opened");
                    return;
                }

                var assetFileOp = ResolveAssetFile(options.AssetFileName, dbFileName);
                var assetFile = assetFileOp == null ? null : await assetFileOp;

                // NoMutex means SQLite can be safely used by multiple threads provided that no
                // single database connection is used simultaneously in two or more threads.
                SQLite.Net.Interop.SQLiteOpenFlags sqlOpenFlags = SQLite.Net.Interop.SQLiteOpenFlags.NoMutex;
                string absoluteDbPath;
                if (options.ReadOnly && assetFileOp != null)
                {
                    sqlOpenFlags |= SQLite.Net.Interop.SQLiteOpenFlags.ReadOnly;
                    absoluteDbPath = assetFile.Path;
                }
                else
                {
                    sqlOpenFlags |= SQLite.Net.Interop.SQLiteOpenFlags.ReadWrite | SQLite.Net.Interop.SQLiteOpenFlags.Create;
                    absoluteDbPath = ResolveDbFilePath(dbFileName);

                    // Option to create from resource (pre-populated) if db does not exist:
                    if (assetFileOp != null)
                    {
                        try
                        {
                            await CopyDbAsync(assetFile, dbFileName);
                        }
                        catch (Exception)
                        {
                            // CopyDbAsync throws when the file already exists.
                        }
                    }
                }

                SQLite.Net.Interop.IDbHandle dbHandle;
                if (_sqliteAPI.Open(GetNullTerminatedUtf8(absoluteDbPath), out dbHandle, (int)sqlOpenFlags, IntPtr.Zero) == SQLite.Net.Interop.Result.OK)
                {
                    _openDBs[dbFileName] = new OpenDB(dbHandle, absoluteDbPath);
                    success.Invoke("Database opened");
                }
                else
                {
                    error.Invoke("Unable to open DB");
                }
            });
        }

        public class CloseOptions
        {
            public string path { get; set; }
        }

        [ReactMethod]
        public void close(CloseOptions options, Action<string> success, Action<string> error)
        {
            QueueWithCancellation(() =>
            {
                var dbFileName = options.path;

                if (dbFileName == null)
                {
                    error.Invoke("You must specify database path");
                    return Task.CompletedTask;
                }

                if (!_openDBs.ContainsKey(dbFileName))
                {
                    error.Invoke("Specified db was not open");
                    return Task.CompletedTask;
                }

                var dbInfo = _openDBs[dbFileName];
                _openDBs.Remove(dbFileName);

                if (_sqliteAPI.Close(dbInfo.Handle) != SQLite.Net.Interop.Result.OK)
                {
                    Debug.WriteLine("SQLitePluginModule: Error closing database: " + dbInfo.Path);
                }

                success.Invoke("DB closed");
                return Task.CompletedTask;
            });
        }

         public class DBConfigOptions
        {
            public string path { get; set; }
            public string dbName { get; set; }
            public string dbAlias { get; set; }
        }

        [ReactMethod]
        public void attach(JSValue options, Action<string> success, Action<string> error)
        {
            QueueWithCancellation(() =>
            {
                var isError = true;
                DBConfigOptions configOptions = options.To<DBConfigOptions>();
                
                if (configOptions != null)
                {
                    var dbFileName = configOptions.path;
                    var dbName = configOptions.dbName;
                    var dbAlias = configOptions.dbAlias;
                    if (dbFileName != null || dbName != null || dbAlias != null)
                    {
                        var dbInfo = _openDBs[dbFileName];
                        var dbInfoToAttach = _openDBs[dbName];

                        var dbPathToAttach = dbInfoToAttach.Path;
                        var SQL = "ATTACH '" + dbPathToAttach + "' AS " + dbAlias;

                        DBQuery query = new DBQuery();
                        query.sql = SQL;
                        query.qid = 1;
                        try
                        {
                            var rawResult = ExecuteQuery(dbInfo, query);
                            isError = false;
                        }
                        catch (RNSQLiteException ex)
                        {
                            System.Diagnostics.Debug.WriteLine("SQLitePluginModule: Error attaching database: " + dbInfo.Path);
                        }
                    }
                }

                if (isError)
                {
                    error.Invoke("Database attachment is failed");
                }
                else
                {
                    success.Invoke("Database attachment is success");
                }
                return Task.CompletedTask;
            });
        }

        public class DeleteOptions
        {
            public string path { get; set; }
        }

        [ReactMethod]
        public void delete(DeleteOptions options, Action<string> success, Action<string> error)
        {
            QueueWithCancellation(async () =>
            {
                var dbFileName = options.path;

                if (dbFileName == null)
                {
                    error.Invoke("You must specify database path");
                    return;
                }
                
                if (_openDBs.ContainsKey(dbFileName))
                {
                    var dbInfo = _openDBs[dbFileName];
                    _openDBs.Remove(dbFileName);

                    if (_sqliteAPI.Close(dbInfo.Handle) != SQLite.Net.Interop.Result.OK)
                    {
                        System.Diagnostics.Debug.WriteLine("SQLitePluginModule: Error closing database: " + dbInfo.Path);
                    }
                }

                var absoluteDbPath = ResolveDbFilePath(dbFileName);
                try
                {
                    var dbFile = await Windows.Storage.StorageFile.GetFileFromPathAsync(absoluteDbPath);
                    await dbFile.DeleteAsync();
                }
                catch (FileNotFoundException)
                {
                    error.Invoke("The database does not exist on that path");
                    return;
                }

                success.Invoke("DB deleted");
            });
        }

        public class DBArgs
        {
            public string dbname { get; set; }
        }

        public class DBQuery
        {
            public int qid { get; set; }
            public JSValue Params { get; set; } // optional
            public string sql { get; set; }
        }

        public class ExecuteSqlBatchOptions
        {
            public DBArgs dbargs { get; set; }
            public List<DBQuery> executes { get; set; }
        }

        private void BindStatement(SQLite.Net.Interop.IDbStatement statement, int argIndex, JSValue arg)
        {
            switch (arg.Type)
            {
                case JSValueType.Null:
                    _sqliteAPI.BindNull(statement, argIndex);
                    break;
                case JSValueType.Boolean:
                    _sqliteAPI.BindInt(statement, argIndex, arg.To<bool>() ? 1 : 0);
                    break;
                case JSValueType.Int64:
                    _sqliteAPI.BindInt64(statement, argIndex, arg.To<long>());
                    break;
                case JSValueType.Double:
                    _sqliteAPI.BindDouble(statement, argIndex, arg.To<double>());
                    break;
                case JSValueType.String:
                    _sqliteAPI.BindText16(statement, argIndex, arg.To<string>(), -1, NegativePointer);
                    break;
                default:
                    _sqliteAPI.BindText16(statement, argIndex, arg.To<string>(), -1, NegativePointer);
                    break;
            }
        }

        private JSValue ExtractColumn(SQLite.Net.Interop.IDbStatement statement, int columnIndex)
        {
            var columnType = _sqliteAPI.ColumnType(statement, columnIndex);
            switch (columnType)
            {
                case SQLite.Net.Interop.ColType.Integer:
                    return new JSValue(_sqliteAPI.ColumnInt64(statement, columnIndex));
                case SQLite.Net.Interop.ColType.Float:
                    return new JSValue(_sqliteAPI.ColumnDouble(statement, columnIndex));
                case SQLite.Net.Interop.ColType.Text:
                    return new JSValue(_sqliteAPI.ColumnText16(statement, columnIndex));
                case SQLite.Net.Interop.ColType.Null:
                default:
                    return new JSValue();
            }
        }

        private Dictionary<string, JSValue> ExtractRow(SQLite.Net.Interop.IDbStatement statement)
        {
            var row = new Dictionary<string, JSValue>();
            var columnCount = _sqliteAPI.ColumnCount(statement);
            for (var i = 0; i < columnCount; i++)
            {
                var columnName = _sqliteAPI.ColumnName16(statement, i);
                JSValue columnValue = ExtractColumn(statement, i);
                row[columnName] = columnValue;
            }
            return row;
        }

        public delegate void SQLiteErrorEvent(SQLiteError error);

        public event SQLiteErrorEvent OnSQLiteError;

        private bool _isExecutingQuery = false;
        private Dictionary<string, JSValue> ExecuteQuery(OpenDB dbInfo, DBQuery query)
        {
            Debug.Assert(!_isExecutingQuery, "SQLitePluginModule: Only 1 query should be executing at a time.");

            _isExecutingQuery = true;
            try
            {
                if (query.sql == null)
                {
                    throw new RNSQLiteException("You must specify a sql query to execute");
                }

                try
                {
                    var previousRowsAffected = _sqliteAPI.TotalChanges(dbInfo.Handle);

                    var statement = _sqliteAPI.Prepare2(dbInfo.Handle, query.sql);
                    if (!query.Params.IsNull)
                    {
                        var argIndex = 0;
                        foreach (var arg in query.Params.To<List<JSValue>>())
                        {
                            // sqlite bind uses 1-based indexing for the arguments
                            BindStatement(statement, argIndex + 1, arg);
                            argIndex++;
                        }
                    }

                    var resultRows = new List<JSValue>();

                    string insertId = null;
                    var rowsAffected = 0;
                    SQLiteError error = null;
                    var keepGoing = true;
                    while (keepGoing)
                    {
                        switch (_sqliteAPI.Step(statement))
                        {
                            case SQLite.Net.Interop.Result.Row:
                                resultRows.Add(new JSValue(ExtractRow(statement)));
                                break;

                            case SQLite.Net.Interop.Result.Done:
                                var nowRowsAffected = _sqliteAPI.TotalChanges(dbInfo.Handle);
                                rowsAffected = nowRowsAffected - previousRowsAffected;
                                var nowInsertId = _sqliteAPI.LastInsertRowid(dbInfo.Handle);
                                if (rowsAffected > 0 && nowInsertId != 0)
                                {
                                    insertId = nowInsertId.ToString();
                                }
                                keepGoing = false;
                                break;

                            default:
                                var webErrorCode = sqliteToWebSQLError(_sqliteAPI.ErrCode(dbInfo.Handle));
                                var message = _sqliteAPI.Errmsg16(dbInfo.Handle);
                                error = new SQLiteError(webErrorCode, message);
                                keepGoing = false;
                                break;
                        }
                    }

                    _sqliteAPI.Finalize(statement);

                    if (error != null)
                    {
                        NotifyOnSQLiteException(error);
                        throw new RNSQLiteException(error);
                    }

                    var resultSet = new Dictionary<string, JSValue>
                    {
                        { "rows", new JSValue(resultRows) },
                        { "rowsAffected", new JSValue(rowsAffected) }
                    };
                    if (insertId != null)
                    {
                        resultSet["insertId"] = new JSValue(insertId);
                    }
                    return resultSet;
                }
                catch (SQLite.Net.SQLiteException ex)
                {
                    var error = new SQLiteError(sqliteToWebSQLError(ex.Result), ex.Message);
                    NotifyOnSQLiteException(error);
                    throw new RNSQLiteException(error);
                }
            }
            finally
            {
                _isExecutingQuery = false;
            }
        }

        private void NotifyOnSQLiteException(SQLiteError error)
        {
            try
            {
                OnSQLiteError?.Invoke(error);
            }
            catch (Exception)
            {
                // no-op
            }
        }

        [ReactMethod]
        public void executeSqlBatch(ExecuteSqlBatchOptions options, Action<JSValue> success, Action<string> error)
        {
            QueueWithCancellation(() =>
            {
                var dbFileName = options.dbargs.dbname;

                if (dbFileName == null)
                {
                    error.Invoke("You must specify database path");
                    return Task.CompletedTask;
                }

                OpenDB dbInfo;
                if (!_openDBs.TryGetValue(dbFileName, out dbInfo))
                {
                    error.Invoke("No such database, you must open it first");
                    return Task.CompletedTask;
                }

                var results = new List<JSValue>();
                foreach (var query in options.executes)
                {
                    try
                    {
                        var rawResult = ExecuteQuery(dbInfo, query);
                        results.Add(new JSValue(new Dictionary<string, JSValue>
                    {
                        { "qid", new JSValue(query.qid) },
                        { "type", new JSValue("success") },
                        { "result", new JSValue(rawResult) }
                    }));
                    }
                    catch (RNSQLiteException ex)
                    {
                        results.Add(new JSValue(new Dictionary<string, JSValue>
                    {
                        { "qid", new JSValue(query.qid) },
                        { "type", new JSValue("error") },
                        { "error", new JSValue(ex.JsonMessage.ToString()) },
                        { "result", new JSValue(ex.JsonMessage.ToString()) }
                    }));
                    }
                }

                success.Invoke(new JSValue(results));
                return Task.CompletedTask;
            });
        }

        [ReactMethod]
        public void backgroundExecuteSqlBatch(ExecuteSqlBatchOptions options, Action<JSValue> success, Action<string> error)
        {
            // Currently, all ReactMethods are run on the same ActionQueue. This prevents
            // queries from being able to run in parallel but it makes the code simpler.
            //
            // `executeSqlBatch` takes care of putting the work on the awaiting queue
            // so we don't have to.
            executeSqlBatch(options, success, error);
        }
    }
}
