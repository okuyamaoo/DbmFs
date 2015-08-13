package dbmfs;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;

import org.dbmfs.*;
import org.dbmfs.query.*;


/**
 * Unit test for simple App.
 */
public class AppTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        DatabaseFilesystem.driverName = null;
        DatabaseFilesystem.databaseUrl = "jdbc:mysql://localhost/test";
        DatabaseFilesystem.databaseAddress = "localhost";
        DatabaseFilesystem.databasePort = -1;
        DatabaseFilesystem.databaseName = "test";
        DatabaseFilesystem.user = "root";
        DatabaseFilesystem.password = "";
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        long prefix = System.currentTimeMillis();
        StringBuilder tableNameBuf = new StringBuilder();
        String tableName = null;
        tableNameBuf.append("tbl");
        tableNameBuf.append(prefix);
        tableName = tableNameBuf.toString();
        Connection conn = null;
        try {
            conn = getDbConnection();
            // テーブル作成
            QueryRunner qr = new QueryRunner();
            qr.update(conn, "create table " + tableName + "(col1 int not null primary key, val1 varchar(100),  val2 varchar(100))");

            qr = new QueryRunner();
            qr.update(conn, "insert into " + tableName + "(col1, val1, val2) values(1, 'val1', 'val2')");
            qr = new QueryRunner();
            qr.update(conn, "insert into " + tableName + "(col1, val1, val2) values(2, 'val11', 'val22')");
            qr = new QueryRunner();
            qr.update(conn, "insert into " + tableName + "(col1, val1, val2) values(3, 'val111', 'val222')");
            conn.close();
            conn = null;
        } catch (Exception e) {
            try {
                conn = getDbConnection();
                QueryRunner qr2 = new QueryRunner();
                qr2.update(conn, "drop table " + tableName);
                conn.close();
                conn = null;
            } catch (Exception e2) {
                e2.printStackTrace();
            }
            // DBに繋がらないのでテストケーススキップ
            e.printStackTrace();
            assertTrue( true );
            System.out.println("データベースに繋がらないためテストをスキップ");
            System.out.println(" テストを成功させる場合MySQLにて root というユーザ名にて");
            System.out.println(" ログイン出来る test というスキーマ名のデータベースをローカルで");
            System.out.println(" 起動してください。");
            System.out.println(" 自動的にテーブルを作成しDropするため注意してください。テーブル名はtbl+(Unix timestamp）");
            System.out.println(" テスト成功時はテーブルは自動的に削除されます。");

            return ;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e3) {
                    e3.printStackTrace();
                }
            }
        }

        try {
            // DBに繋がる前提
            DatabaseClient dbClient = new DatabaseClient(new BindQueryFolder());
            // Topディレクトリにテーブルがあることを確認
            Map<String, String> dirMap = dbClient.getDirectoryInObjects("/");
            if (!dirMap.containsKey("/" + tableName)) {
                assertTrue( false );
                return ;
            }
            String dirStr = dirMap.get("/" + tableName);

            if (!dirStr.equals("dir")) {
                assertTrue( false );
                return ;
            }

            Map<String, String> dirMap2 = dbClient.getDirectoryInObjects("/" + tableName);

            if (!dirMap2.containsKey("/" + tableName + "/1.json")) {
                assertTrue( false );
                return ;
            }

            if (!dirMap2.containsKey("/" + tableName + "/2.json")) {
                assertTrue( false );
                return ;
            }

            if (!dirMap2.containsKey("/" + tableName + "/2.json")) {
                assertTrue( false );
                return ;
            }

        } catch (Exception e) {
            // DBに繋がらないのでテストケーススキップ
            assertTrue( false );
            e.printStackTrace();
            return ;
        } finally {
            try {
                conn = getDbConnection();
                QueryRunner qr2 = new QueryRunner();
                qr2.update(conn, "drop table " + tableName);
                conn.close();
                conn = null;
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        assertTrue( true );
    }

    private Connection getDbConnection() throws Exception {

        Connection conn = DriverManager.getConnection(DatabaseFilesystem.databaseUrl,
                                                        DatabaseFilesystem.user,
                                                          DatabaseFilesystem.password);
        conn.setAutoCommit(true);
        return conn;
  }

}
