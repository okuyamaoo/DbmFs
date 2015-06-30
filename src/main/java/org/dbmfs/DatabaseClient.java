package org.dbmfs;

import java.util.*;
import java.nio.*;
import java.io.*;

/**
 * DBFS.<br>
 *
 * @author okuyamaoo
 * @license Apache License
 */
public class DatabaseClient {

    /**
     * コンストラクタ
     */
    public DatabaseClient() throws Exception {
    }


    /**
     * 指定されたパス配下に含まれるディレクトリとファイルの情報を返す
     *
     */
    public Map<String, String> getDirectoryInObjects(String path) throws Exception {
            // /a = {/a/1.txt=file, /a/2.txt=file}
            // / = {/a=dir, /3.txt=file}
        Map<String, String> directoryObjects = new LinkedHashMap();
        DatabaseAccessor da = new DatabaseAccessor();

        // Topディレクトリである"/"がpathの場合はテーブル一覧取得
        if (path != null && path.equals("/")) {

            List<String> tableList = da.getTableList();
            for (int idx = 0; idx < tableList.size(); idx++) {
                directoryObjects.put("/" + tableList.get(idx), "dir");
            }
        } else if (path != null) {
            // "/"以外の場合はテーブル指定なので、テーブル名として利用しSQL実行。データ一覧取得

            // 整合性確認のためテーブル名として指定のpathが存在するか確認
            String[] splitPathList = path.split("/");
            String tableName = "";
            // 分解後の文字列で文字が存在する1つ目の文字列がテーブル名
            if (splitPathList != null && splitPathList.length > 0) {
                for (int idx = 0; idx < splitPathList.length; idx++) {
                    String nextTableStr = splitPathList[idx].trim();
                    if (!nextTableStr.equals("")) {
                        tableName = nextTableStr;
                        break;
                    }
                }

                // テーブル名指定がDBに存在するテーブル名か確認
                if (da.exsistTable(tableName)) {
                    // テーブル名として存在するため主キーの連結文字列を取得
                    List<String> pKeyConcatStrList = da.getRecordKeyList(tableName);
                    for (int idx = 0; idx < pKeyConcatStrList.size(); idx++) {

                        String pKeyConcatStr = pKeyConcatStrList.get(idx);
                        directoryObjects.put(DbmfsUtil.createFileFullPathString(tableName, pKeyConcatStr), "file");
                    }
                }
            }
        }
        return directoryObjects;
    }

    /**
     * 引数のパスの表すオブジェクトの詳細情報を返す
     *
     */
    public String getInfomation(String key) throws Exception {
        String retStr = null;

//   /a       = dir    1  0  0  0  1435098875  0  493    0  24576974580222
//   /a/1.txt = file  1  0  0  0  1435098888  0  33188  0  24589836752449  -1
//   /a/2.txt = file  1  0  0  0  1435098890  0  33188  0  24591395798630  -1
//   /a.txt   = file  1  0  0  40  1435101323  1  33188  0  27024968062029  0

        // Topディレクトリである"/"がpathの場合はテーブル一覧取得
        if (key != null && key.equals("/")) return null;

        StringBuilder strBuf = new StringBuilder();

        if (key != null) {

            // key変数をディレクトリ名だけもしくは、ディレクトリ名とファイル名の配列に分解する
            String[] splitPath = DbmfsUtil.splitTableNameAndPKeyCharacter(key);

            DatabaseAccessor da = new DatabaseAccessor();

            // 分解した文字列は正しい場合はsplitPath[0]:テーブル名、splitPath[1]:データファイル.jsonもしくはsplitPath[0]:テーブル名のはず
            if (splitPath.length == 1) {
                // テーブル名のみ
                if (da.exsistTable(splitPath[0]))
                    // ディレクトリ用のfstat用文字列を作成し返す
                    return DbmfsUtil.createDirectoryInfoTemplate();

            } else if (splitPath.length == 2) {
                // テーブル名とデータファイル名
                if (da.exsistTable(splitPath[0])) {

                    // テーブルが存在する
                    // ファイル名からデータを特定
                    // ファイル名には主キー + ".json"が付加されているので取り外す
                    String realPKeyString = DbmfsUtil.deletedFileTypeCharacter(splitPath[1]);
                    List<Map<String, Object>> dataList = da.getDataList(splitPath[0], realPKeyString);

                    if (dataList == null) return null;

                    // JSON文字列化
                    String dataString = DbmfsUtil.jsonSerialize(dataList);
                    byte[] strBytes = dataString.getBytes();

                    // ファイル用のfstat用文字列を作成し返す
                    return DbmfsUtil.createFileInfoTemplate(strBytes.length);
                }
            } else {
                // 不正な指定
                return null;
            }
        }
        return null;
    }


    /**
     * 指定されたパスのファイルが表すDB上のデータを取得し指定されたByteBufferへ値を入れる
     *
     *
     */
    public int readValue(String key, long offset, int limit, ByteBuffer buf) throws Exception {

        String dataString = null;

        if (key != null && !key.trim().equals("") && !key.trim().equals("/")) {
            // ファイル名のフルパスからテーブル名と単独ファイル名に分解
            String[] pathSplit = DbmfsUtil.splitTableNameAndPKeyCharacter(key);
            if (pathSplit.length == 2) {

                // 正しい指定
                // ファイル名から拡張子取り外し
                String pKeyStr = DbmfsUtil.deletedFileTypeCharacter(pathSplit[1]);

                DatabaseAccessor da = new DatabaseAccessor();
                // データ取得
                List<Map<String, Object>> dataList = da.getDataList(pathSplit[0], pKeyStr);
                if (dataList == null) return 0;

                // JSON文字列化
                dataString = DbmfsUtil.jsonSerialize(dataList);

                byte[] strBytes = dataString.getBytes();
                if (strBytes.length < offset) return 0; // データサイズを指定位置が超えている。これはファイルのサイズを無条件で1MBとしているため。

                int readLen = -1;
                byte[] retData = null;
                if (strBytes.length < (offset + limit)) {
                    retData = new byte[new Long((new Long(strBytes.length).longValue() - offset)).intValue()];
                    System.arraycopy(strBytes, new Long(offset).intValue(), retData, 0, retData.length);
                    buf.put(retData);
                } else {
                    retData = new byte[limit];
                    System.arraycopy(strBytes, new Long(offset).intValue(), retData, 0, retData.length);
                    buf.put(retData);
                }
                return retData.length;
            }
        }
        return -1;
    }
}
