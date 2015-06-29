package org.dbmfs;

import java.util.*;

import fuse.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

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
		 * -dburl MySQLへの接続文字列。DB名まで含む指定。またポート番号なども変えている場合はそちらも含める 例(-dburl jdbc:mysql://localhost/test)
		 * -dbuser MySQLのデータベースへの接続ユーザ。 例(-dbuser testuser)
		 * -dbpass MySQLのデータベースへの接続ユーザのパスワード。 例(-dbpass password)
		 *
		 */
    public static void main(String[] args) {

        Map<String, String> dbmfsParams = new HashMap();
        List<String> fuseParams = new ArrayList();

				try {

						compileBootArgument(args, dbmfsParams, fuseParams);
						if (dbmfsParams.size() != 3) throw new Exception();
				} catch (Exception e) {
						printBootMessageError();
						System.exit(1);
				}

        try {

            DatabaseFilesystem dbfs = new DatabaseFilesystem("com.mysql.jdbc.Driver", dbmfsParams.get("dburl"), dbmfsParams.get("dbuser"), dbmfsParams.get("dbpass"));
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
		private static void compileBootArgument(String[] args, Map<String, String> dbmfsParams, List<String> fuseParams) {
				dbmfsParams.put("dbpass", ""); // パスワードは省略有り

				for (int idx = 0; idx < args.length; idx++) {
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
				System.out.println("                org.dbmfs.DbmFs -dburl jdbc:mysql://localhost/test -dbuser root -dbpass *********");
		}
}
