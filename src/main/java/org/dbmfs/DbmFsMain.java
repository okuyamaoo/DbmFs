package org.dbmfs;

import java.util.*;

import fuse.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.dbmfs.query.*;

/**
 * DbmFsMain.<br>
 *
 *
 * @author okuyamaoo
 * @license Apache license
 */
public class DbmFsMain {

    private static final Log log = LogFactory.getLog(DbmFsMain.class);

    /**
     * DbmFs起動.<br>
     * 起動引数は、fuseのオプション系とマウントパスそれ以外は以下<br>
     * -dbdriver データベース(MySQL or PostgreSQL)のJDBCドライバー文字列  省略時はMySQLとなる 例(-dbdriver org.postgresql.Driver)
     * -dburl データベースへの接続文字列。DB名まで含む指定。またポート番号なども変えている場合はそちらも含める 例(-dburl jdbc:mysql://localhost/test)
     * -dbuser データベースのデータベースへの接続ユーザ。 例(-dbuser testuser)
     * -dbpass データベースのデータベースへの接続ユーザのパスワード。 例(-dbpass password)
     * -viewdir テーブル名とSelectクエリを指定し仮想的なテーブルを作成する(Viewのイメージ)
     *
     */
    public static void main(String[] args) {

        Map<String, String> dbmfsParams = new HashMap();
        List<String> fuseParams = new ArrayList();
        BindQueryFolder bindQueryFolder = new BindQueryFolder();
        try {

            compileBootArgument(args, dbmfsParams, fuseParams, bindQueryFolder);
            if (dbmfsParams.size() != 5) throw new Exception();
        } catch (Exception e) {
            printBootMessageError();
            System.exit(1);
        }

        try {
            Runtime.getRuntime().addShutdownHook(new ShutdownProccess());

            DatabaseFilesystem dbfs = new DatabaseFilesystem(dbmfsParams.get("dbdriver"), dbmfsParams.get("dburl"), dbmfsParams.get("dbuser"), dbmfsParams.get("dbpass"), dbmfsParams.get("readonly"), dbmfsParams.get("mttables"), bindQueryFolder);
            FuseMount.mount(fuseParams.toArray(new String[0]), dbfs, log);
        } catch (Exception e) {
           e.printStackTrace();
        }
    }


    /**
     * 起動時のパラメータエラーのコンパイル
     * TODO 簡易
     *
     */
    private static void compileBootArgument(String[] args, Map<String, String> dbmfsParams, List<String> fuseParams, BindQueryFolder bindQueryFolder) {
        dbmfsParams.put("dbpass", ""); // パスワードは省略有り
        dbmfsParams.put("dbdriver", "com.mysql.jdbc.Driver"); // デフォルトドライバーはMySQL
        dbmfsParams.put("readonly", "0");

        for (int idx = 0; idx < args.length; idx++) {
            if (args[idx].indexOf("-dbdriver") == 0) {
                idx++;
                dbmfsParams.put("dbdriver", args[idx]);
                continue;
            }

            if (args[idx].indexOf("-dburl") == 0) {
                idx++;
                dbmfsParams.put("dburl", args[idx]);
                continue;
            }

            if (args[idx].indexOf("-dbuser") == 0) {
                idx++;
                dbmfsParams.put("dbuser", args[idx]);
                continue;
            }

            if (args[idx].indexOf("-dbpass") == 0) {
              idx++;
              dbmfsParams.put("dbpass", args[idx]);
              continue;
            }

            if (args[idx].indexOf("-mttables") == 0) {
              idx++;
              dbmfsParams.put("mttables", args[idx]);
              continue;
            }

            // BindQueryを設定
            // 指定フォーマットは
            // フォルダ名 + @ + SQL文字列
            if (args[idx].indexOf("-viewdir") == 0) {
              idx++;
              String[] bindQueryInfo = args[idx].split("/");
              bindQueryFolder.addBindQuery(bindQueryInfo[0], bindQueryInfo[1], bindQueryInfo[2]);
              continue;
            }

            // ReadOnlyマウントの有無
            if (args[idx].indexOf("-readonly") == 0) {
              idx++;
              if (args[idx].trim().toLowerCase().equals("true")) dbmfsParams.put("readonly", "1");
              continue;
            }

            fuseParams.add(args[idx]);
        }
    }


    /**
     * 起動時のパラメータエラー出力
     * TODO 簡易
     *
     */
    private static void printBootMessageError() {
        System.out.println("Start argument is invalid !!");
        System.out.println("Usage : java ~~ org.dbmfs.DbmFs -dburl jdbc:mysql://localhost/test -dbuser root");
        System.out.println("                                   or                                          ");
        System.out.println("Usage : java ~~ org.dbmfs.DbmFs -dburl jdbc:mysql://localhost/test -dbuser root -viewdir queryfoldername/select column1,column2 from tablename/PrimaryKeyColumnNames");
        System.out.println("                                   or                                          ");
        System.out.println("                org.dbmfs.DbmFs -dburl jdbc:mysql://localhost/test -dbuser root -dbpass *********");
        System.out.println("                                   or                                          ");
        System.out.println("                org.dbmfs.DbmFs -dbdriver org.postgresql.Driver -dburl jdbc:postgresql://localhost/test -dbuser postgres");

    }
}
