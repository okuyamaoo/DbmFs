package org.dbmfs;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.ParameterMetaData;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

/**
 * DBアクセス用のクラス.<br>
 * TODO:MySQLに特化しているので、後でInterface化して実実装は移動する<br>
 *
 * @author okuyamaoo
 * @license Apache License
 */
public class DatabaseAccessor {

    public static String tableNameSep = "___";  // キャッシュ用のキーのセパレータ

    public static String primaryKeySep = "_#_";  // 主キー連結用のセパレータ

    public static CacheFolder tableListCacheFolder = new CacheFolder();
    public static CacheFolder tableExsistCacheFolder = new CacheFolder();
    public static CacheFolder pKeyColumnNameCacheFolder = new CacheFolder();
    public static CacheFolder allColumnMetaCacheFolder = new CacheFolder();
    public static CacheFolder dataCacheFolder = new CacheFolder();

    private Connection injectConn = null;

    private Map<String, Integer> sqlTypeMap = null;
    /**
     * コンストラクタ
     */
    public DatabaseAccessor() {}

    /**
     * コンストラクタ
     */
    public DatabaseAccessor(Connection conn) {
        injectConn = conn;
    }

    protected void initSqlTypeMap() {
        sqlTypeMap = new HashMap();
    }

    /**
     * テーブルのリスト情報返却.<br>
     *
     * @return テーブル名のリスト
     */
    public List<String> getTableList() throws Exception {
        if (tableListCacheFolder.containsKey("tablelist")) return (List<String>)tableListCacheFolder.get("tablelist");

        List<String> tableNameList = new ArrayList();
        Connection conn = null;
        try {
            String table = "%";
            String types[] = { "TABLE", "VIEW", "SYSTEM TABLE" };

            conn = getDbConnection();
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, table, types);
            while (rs.next()) {

                String tableName= rs.getString("TABLE_NAME");
                tableNameList.add(tableName);
            }

            tableListCacheFolder.put("tablelist", tableNameList);
            rs.close();
            conn.close();
            conn = null;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (conn != null) conn.close();
            }  catch(Exception e2) {}
        }

        return tableNameList;
    }


    /**
     * 引数のテーブル名が存在するかを確認.<br>
     *
     * @param targetTableName テーブル名
     * @return 存在有無
     */
    public boolean exsistTable(String targetTableName) throws Exception {
        if (tableExsistCacheFolder.containsKey(targetTableName)) return true;
        boolean ret = false;
        Connection conn = null;
        try {
            String table = "%";
            String types[] = { "TABLE", "VIEW", "SYSTEM TABLE" };

            conn = getDbConnection();
            DatabaseMetaData dbmd = conn.getMetaData();
            ResultSet rs = dbmd.getTables(null, null, table, types);
            while (rs.next()) {

                String tableName= rs.getString("TABLE_NAME");
                if (targetTableName.equals(tableName)) {
                    ret =true;

                    tableExsistCacheFolder.put(targetTableName, "true");
                    break;
                }
            }

            rs.close();
            conn.close();
            conn = null;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (conn != null) conn.close();
            }  catch(Exception e2) {}
        }
        return ret;
    }


    /**
     * テーブル名と主キーの連結文字列から該当データが存在するかを判定し返す.<br>
     *
     * @param targetTableName テーブル名
     * @param pKeyConcatStr 主キー文字列(連結文字列)
     * @retrun データ存在有無
     */
    public boolean exsistData(String targetTableName, String pKeyConcatStr) throws Exception {
        boolean ret = false;
        Connection conn = null;
        try {

            // プライマリーキー取得
            List<String> primaryKeyColumnNames = getPrimaryKeyColumnNames(targetTableName);

            if (primaryKeyColumnNames == null || primaryKeyColumnNames.size() == 0) return false;

            // 主キー連結文字列を分解
            String[] keyStrSplit = pKeyConcatStr.split(primaryKeySep);

            if (keyStrSplit.length != primaryKeyColumnNames.size()) return false;

            // クエリ組み立て
            StringBuilder queryBuf = new StringBuilder();
            queryBuf.append("select count(*) as cnt from ");
            queryBuf.append(targetTableName);
            queryBuf.append(" where ");

            // クエリパラメータ(主キー)作成
            Object[] params = new Object[primaryKeyColumnNames.size()];

            String whereSep = "";
            for (int idx = 0; idx < primaryKeyColumnNames.size(); idx++) {
                params[idx] = keyStrSplit[idx];
                queryBuf.append(whereSep);
                queryBuf.append(primaryKeyColumnNames.get(idx));
                queryBuf.append(" = ? ");
                whereSep = " and ";
            }

            conn = getDbConnection();
            ResultSetHandler<?> resultSetHandler = new MapListHandler();
            QueryRunner qr = new QueryRunner();

            // クエリ実行
            List<Map<String, Object>> queryResult = (List<Map<String, Object>>)qr.query(conn, queryBuf.toString(), resultSetHandler, params);

            Map countRet = queryResult.get(0);
            Long count = (Long)countRet.get("cnt");

            if (count.longValue() == 1) ret = true;

            conn.close();
            conn = null;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (conn != null) conn.close();
            }  catch(Exception e2) {}
        }

        return ret;
    }


    /**
     * テーブル名とキーの連結文字列から該当データMap型をリストに詰めて返す.<br>
     * データが存在しない場合やエラー発生時はnullを返す<br>
     *
     * @param targetTableName テーブル名
     * @param pKeyConcatStr 主キー文字列(連結文字列)
     * @return 複数件のデータ
     */
    public List<Map<String, Object>> getDataList(String targetTableName, String pKeyConcatStr) throws Exception {
        List <Map<String, Object>> queryResult = null;
        Connection conn = null;
        try {
            // プライマリーキー取得
            List<String> primaryKeyColumnNames = getPrimaryKeyColumnNames(targetTableName);

            if (primaryKeyColumnNames == null || primaryKeyColumnNames.size() == 0) return null;


            // 主キー連結文字列を分解
            String[] keyStrSplit = pKeyConcatStr.split(primaryKeySep);

            // 主キー連結文字列を分解した数と主キーの数が一致しない場合はデータ無しで返却
            if (primaryKeyColumnNames.size() != keyStrSplit.length) return null;

            // キャッシュよりデータ取得
            StringBuilder cacheKeyBuf = new StringBuilder(30);
            cacheKeyBuf.append(targetTableName);
            cacheKeyBuf.append(tableNameSep);
            cacheKeyBuf.append(pKeyConcatStr);

            Object cacheDataMap = dataCacheFolder.get(cacheKeyBuf.toString());
            if (cacheDataMap != null) {
                if (cacheDataMap instanceof Map) {
                    List<Map<String, Object>> cacheList =  new ArrayList();
                    cacheList.add((Map<String, Object>)cacheDataMap);
                    return cacheList;
                } else if (cacheDataMap instanceof List) {
                    return (List<Map<String, Object>>)cacheDataMap;
                }
            }

            // クエリ組み立て
            StringBuilder queryBuf = new StringBuilder();
            queryBuf.append("select * from ");
            queryBuf.append(targetTableName);
            queryBuf.append(" where ");

            // クエリパラメータ(主キー)作成
            Object[] params = new Object[primaryKeyColumnNames.size()];

            String whereSep = "";
            for (int idx = 0; idx < primaryKeyColumnNames.size(); idx++) {

                params[idx] = keyStrSplit[idx];
                queryBuf.append(whereSep);
                queryBuf.append(primaryKeyColumnNames.get(idx));
                queryBuf.append(" = ? ");
                whereSep = " and ";
            }

            conn = getDbConnection();
            ResultSetHandler<?> resultSetHandler = new MapListHandler();
            QueryRunner qr = new QueryRunner();

            // クエリ実行
            queryResult = (List<Map<String, Object>>)qr.query(conn, queryBuf.toString(), resultSetHandler, params);

            if (queryResult == null || queryResult.size() < 1) return null;

            dataCacheFolder.put(cacheKeyBuf.toString(), queryResult);
            conn.close();
            conn = null;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (conn != null) conn.close();
            }  catch(Exception e2) {}
        }

        return queryResult;
    }


    // テーブル名を指定して主キーのリストをメタ情報から取得
    private List<String> getPrimaryKeyColumnNames(String tableName) throws Exception {
        if (pKeyColumnNameCacheFolder.containsKey(tableName)) return (List<String>)pKeyColumnNameCacheFolder.get(tableName);

        Connection conn = null;
        List<String> primaryKeyColumnNames = null;
        try {
            primaryKeyColumnNames = new ArrayList();

            conn = getDbConnection();
            DatabaseMetaData dbmd = conn.getMetaData();

            // プライマリーキー取得
            ResultSet rs = dbmd.getPrimaryKeys(null, null, tableName);
            while (rs.next()) {
                primaryKeyColumnNames.add(rs.getString("COLUMN_NAME"));
            }

            pKeyColumnNameCacheFolder.put(tableName, primaryKeyColumnNames);
            rs.close();
            conn.close();
            conn = null;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (conn != null) conn.close();
            }  catch(Exception e2) {}
        }
        return primaryKeyColumnNames;
    }



    // テーブル名を指定して全カラムのリストをメタ情報から取得
    private Map<String, Map<String, Object>> getAllColumnMeta(String tableName) throws Exception {
        if (allColumnMetaCacheFolder.containsKey(tableName)) return (Map<String, Map<String, Object>>)allColumnMetaCacheFolder.get(tableName);

        Connection conn = null;
        Map<String, Map<String, Object>> allColumnMeta = null;
        try {
            allColumnMeta = new LinkedHashMap();

            conn = getDbConnection();
            DatabaseMetaData dbmd = conn.getMetaData();

            // プライマリーキー取得
            ResultSet rs = dbmd.getColumns(null, null, tableName, "%");
            while (rs.next()) {
                Map<String, Object> columMeta = new LinkedHashMap();
                columMeta.put("name", rs.getString("COLUMN_NAME"));
                columMeta.put("type", rs.getInt("DATA_TYPE"));
                allColumnMeta.put((String)columMeta.get("name"), columMeta);
            }
            System.out.println(allColumnMeta);
            allColumnMetaCacheFolder.put(tableName, allColumnMeta);
            rs.close();
            conn.close();
            conn = null;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (conn != null) conn.close();
            }  catch(Exception e2) {}
        }
        return allColumnMeta;
    }


    /**
     * 指定されたテーブルの全レコードの主キー値の連結文字列を作成する.<br>
     *
     * @param tableName テーブル名
     * @return 主キーを連結した文字列を格納したリスト
     */
    public List<String> getRecordKeyList(String tableName) throws Exception {
        List<String> resultList = new ArrayList();
        Connection conn = null;
        try {

            // プライマリーキー取得
            List<String> primaryKeyColumnNames = getPrimaryKeyColumnNames(tableName);
            // プライマリーキーが存在しないテーブルは扱えない
            if (primaryKeyColumnNames.size() == 0) return resultList;

            // Connection取得
            conn = getDbConnection();


            // プライマリーキーのみをselect句に指定したクエリにてデータ取得
            // TODO:このままだと大量レコードに対応出来ない
            // SQL組み立て
            String query = createAllColumnQuery(tableName);
//            String query = createPrimaryKeyQuery(tableName, primaryKeyColumnNames);
            ResultSetHandler<?> resultSetHandler = new MapListHandler();
            QueryRunner qr = new QueryRunner();

            // クエリ実行
            List<Map<String, Object>> queryResult = (List<Map<String, Object>>)qr.query(conn, query, resultSetHandler);

            // クエリ結果から主キー値を連結した文字列を作り出す
            for (int idx = 0; idx < queryResult.size(); idx++) {

                Map data = queryResult.get(idx);
                StringBuilder queryDataStrBuf = new StringBuilder(40);

                String pKeyStrSep = "";
                for (int pIdx = 0; pIdx < primaryKeyColumnNames.size(); pIdx++) {
                    queryDataStrBuf.append(pKeyStrSep);
                    queryDataStrBuf.append(data.get(primaryKeyColumnNames.get(pIdx)));
                    pKeyStrSep = DatabaseAccessor.primaryKeySep;
                }

                dataCacheFolder.put(tableName + tableNameSep + queryDataStrBuf.toString(), data);
                resultList.add(queryDataStrBuf.toString());
            }

            conn.close();
            conn = null;
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (conn != null) conn.close();
            }  catch(Exception e2) {}
        }
        return resultList;
    }


    public boolean saveData(String tableName, String pKeyConcatStr, Map<String, Object> dataObject) throws Exception {
        try {
            // データの存在を確認
            if (exsistData(tableName, pKeyConcatStr)) {

                // データが存在する
                return updateData(tableName, dataObject);
            } else {

                // データが存在しない
                return insertData(tableName, dataObject);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    public boolean insertData(String tableName, Map<String, Object> dataObject) throws Exception {
        Connection conn = null;

        try {
            Map<String, Map<String, Object>> allColumnMeta = getAllColumnMeta(tableName);
             // QueryParameter
            List queryParams = new ArrayList();
            List<Integer> queryParamTypes = new ArrayList();

            // クエリ組み立て
            StringBuilder queryBuf = new StringBuilder();
            StringBuilder valuesBuf = new StringBuilder();

            queryBuf.append("insert into ");
            queryBuf.append(tableName);
            queryBuf.append(" ( ");
            valuesBuf.append(" values(");
            String sep = "";
            for(Map.Entry<String, Object> ent : dataObject.entrySet()) {
                valuesBuf.append(sep);
                queryBuf.append(sep);

                String columnName = ent.getKey();
                queryBuf.append(columnName);

                Map columnMeta = allColumnMeta.get(columnName);
                if (ent.getValue() != null) {
                    queryParams.add(ent.getValue());
                    queryParamTypes.add((Integer)columnMeta.get("type"));

                    valuesBuf.append("?");
                } else {
                    valuesBuf.append("null");
                }
                sep = ",";
            }

            queryBuf.append(" ) ");
            valuesBuf.append(" ) ");
            queryBuf.append(valuesBuf.toString());

            // パラメータのbind実行
            conn = getDbConnection();
            PreparedStatement preparedStatement = conn.prepareStatement(queryBuf.toString());

            int bindIndex = 1;
            for (Object paramObject : queryParams) {
                if (paramObject == null) {
                    preparedStatement.setNull(bindIndex, queryParamTypes.get(bindIndex - 1).intValue());
                } else {
                    preparedStatement.setObject(bindIndex, paramObject, queryParamTypes.get(bindIndex - 1).intValue());
                }
                bindIndex++;
            }
            preparedStatement.execute();
            preparedStatement.close();
        } catch (SQLException se) {


            se.printStackTrace();
            throw se;

        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (conn != null) conn.close();
            }  catch(Exception e2) {}
        }
        return true;
    }


    /**
     * DBへのUpdate処理
     *
     */
    public boolean updateData(String tableName, Map<String, Object> dataObject) throws Exception {
        Connection conn = null;

        try {
            // テーブル名から主キー取得
            List<String> pKeyNameList = getPrimaryKeyColumnNames(tableName);
            Map<String, Object> pKeyObjectMap = new LinkedHashMap();

            // 主キーのみ保存Objectから取り出し
            for (String pKeyName : pKeyNameList) {
                Object pKeyValue = dataObject.remove(pKeyName);
                if (pKeyValue != null) {
                    pKeyObjectMap.put(pKeyName, pKeyValue);
                }
            }

            // 主キーの存在チェック
            if (pKeyObjectMap.size() == 0) return false;

             // QueryParameter
            List setParams = new ArrayList();
            List whereParams = new ArrayList();

            // クエリ組み立て
            StringBuilder setBuf = new StringBuilder();
            StringBuilder whereBuf = new StringBuilder();

            setBuf.append("update  ");
            setBuf.append(tableName);
            setBuf.append(" set ");
            whereBuf.append(" where ");

            String setSep = "";
            String whereSep = "";

            // UpdateのSetを作成
            for(Map.Entry<String, Object> ent : dataObject.entrySet()) {
                String columnName = ent.getKey();
                Object columnValue = ent.getValue();

                setBuf.append(setSep);

                // Set句を作成
                setBuf.append(columnName);
                setBuf.append(" = ? ");
                setSep = ",";
            }

            // Updateのwhereを作成
            for(Map.Entry<String, Object> ent : pKeyObjectMap.entrySet()) {
                String columnName = ent.getKey();
                Object columnValue = ent.getValue();

                // where句を作成
                whereBuf.append(whereSep);

                if (columnValue == null) {

                    whereBuf.append(columnName);
                    whereBuf.append(" is null ");
                    setParams.add(columnValue);
                } else {

                    whereBuf.append(columnName);
                    whereBuf.append(" = ? ");
                    setParams.add(columnValue);
                    whereParams.add(columnValue);
                }
                whereSep = " and ";
            }

            // クエリ結合
            setBuf.append(whereBuf.toString());
            // パラメータ結合
            setParams.addAll(whereParams);

            // Connection取得
            conn = getDbConnection(false);

            QueryRunner qr = new QueryRunner();
            int updateCount = qr.update(conn, setBuf.toString(), setParams.toArray(new Object[0]));

            if (updateCount != 1) {
                conn.rollback();
                return false;
            } else {
                conn.commit();
                removeDataCache(tableName, pKeyObjectMap);
                return true;
            }
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (conn != null) conn.close();
            }  catch(Exception e2) {}
        }
    }



    /**
     * DBへのDelete処理
     *
     */
    public boolean deleteData(String tableName, String pKeyConcatStr) throws Exception {
      boolean ret = false;
      Connection conn = null;
      try {

          // プライマリーキー取得
          List<String> primaryKeyColumnNames = getPrimaryKeyColumnNames(tableName);

          if (primaryKeyColumnNames == null || primaryKeyColumnNames.size() == 0) return false;

          // 主キー連結文字列を分解
          String[] keyStrSplit = pKeyConcatStr.split(primaryKeySep);

          if (keyStrSplit.length != primaryKeyColumnNames.size()) return false;

          // クエリ組み立て
          StringBuilder queryBuf = new StringBuilder();
          queryBuf.append("delete from ");
          queryBuf.append(tableName);
          queryBuf.append(" where ");

          // クエリパラメータ(主キー)作成
          Object[] params = new Object[primaryKeyColumnNames.size()];

          String whereSep = "";
          for (int idx = 0; idx < primaryKeyColumnNames.size(); idx++) {
              params[idx] = keyStrSplit[idx];
              queryBuf.append(whereSep);
              queryBuf.append(primaryKeyColumnNames.get(idx));
              queryBuf.append(" = ? ");
              whereSep = " and ";
          }

            // Connection取得
            conn = getDbConnection(false);

            QueryRunner qr = new QueryRunner();
            int updateCount = qr.update(conn, queryBuf.toString(), params);

            if (updateCount != 1) {
                conn.rollback();
                return false;
            } else {
                conn.commit();

                // キャッシュよりデータ削除
                removeDataCache(tableName, pKeyConcatStr);
                return true;
            }
        } catch(Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if (conn != null) conn.close();
            }  catch(Exception e2) {}
        }
    }


    /**
     * 主キーを全てselect句に指定したクエリーを作成する
     *
     */
    private String createPrimaryKeyQuery(String tableName, List<String> primaryKeyColumnNames) {
        StringBuilder queryBuf = new StringBuilder();

        String sep = "";

        queryBuf.append("select ");
        for (int idx = 0; idx < primaryKeyColumnNames.size(); idx++) {
            queryBuf.append(sep);
            queryBuf.append(primaryKeyColumnNames.get(idx));
            sep = ",";
        }

        queryBuf.append(" from ");
        queryBuf.append(tableName);
        return queryBuf.toString();
    }


    /**
     * 主キーを全てselect句に指定したクエリーを作成する
     *
     */
    private String createAllColumnQuery(String tableName) {
        StringBuilder queryBuf = new StringBuilder();

        String sep = "";

        queryBuf.append("select *");

        queryBuf.append(" from ");
        queryBuf.append(tableName);
        return queryBuf.toString();
    }

    public void removeDataCache(String tableName, String pKeyConcatStr) {
        StringBuilder cacheKeyBuf = new StringBuilder(30);
        cacheKeyBuf.append(tableName);
        cacheKeyBuf.append(tableNameSep);
        cacheKeyBuf.append(pKeyConcatStr);
        dataCacheFolder.remove(cacheKeyBuf.toString());
    }


    public void removeDataCache(String tableName, Map<String, Object> pKeyDataMap) {
        StringBuilder cacheKeyBuf = new StringBuilder(30);
        cacheKeyBuf.append(tableName);
        cacheKeyBuf.append(tableNameSep);
        String sep = "";

        // 主キー連結文字列作成
        for(Map.Entry<String, Object> ent : pKeyDataMap.entrySet()) {

            Object columnValue = ent.getValue();
            cacheKeyBuf.append(sep);
            cacheKeyBuf.append(columnValue);
            sep = primaryKeySep;

        }
        dataCacheFolder.remove(cacheKeyBuf.toString());
    }

    public Connection getDbConnection() throws Exception {
        return getDbConnection(true);
    }

    public Connection getDbConnection(boolean autoCommit) throws Exception {


        Connection conn = null;

        if (injectConn == null) {
            conn = DriverManager.getConnection(DatabaseFilesystem.databaseUrl,
                                                            DatabaseFilesystem.user,
                                                                DatabaseFilesystem.password);
            conn.setAutoCommit(autoCommit);
        } else {

            conn = injectConn;
        }
        return conn;
  }

}
