package org.dbmfs.query;


import java.util.*;
import java.util.concurrent.*;


public class BindQueryFolder {

    Map<String, String> bindQueryFolder = new ConcurrentHashMap();
    Map<String, List<String>> queryPKeyColumns = new ConcurrentHashMap();
    List<String> bindFolderNames = new ArrayList();

    public BindQueryFolder() {

    }


    /**
     * バインド対象のフォルダ名とクエリを追加する.<br>
     * 同名フォルダ設定した場合は上書きされる<br>
     *
     * @param folderName フォルダ名
     * @param query クエリ文字列(select文であること)
     */
    public void addBindQuery(String folderName, String query, String pKeyColumnNameStr) {

        if (this.bindQueryFolder.put(folderName, query) == null) {
            this.bindFolderNames.add(folderName);

            String[] pKeyColumnNames = pKeyColumnNameStr.split(",");
            List<String> pKeyColumnNameList = new ArrayList();
            for (int idx = 0; idx < pKeyColumnNames.length; idx++) {
                if (!pKeyColumnNames[idx].trim().equals("")) {
                    pKeyColumnNameList.add(pKeyColumnNames[idx].trim());
                }
            }
            queryPKeyColumns.put(folderName, pKeyColumnNameList);
        }
    }


    /**
     * バインド対象のフォルダ名一覧を返す.<br>
     * 存在しない場合は0個の要素のリスト<br>
     *
     * @return フォルダ名のリスト
     */
    public List<String> getBindFolderNames() {
        return this.bindFolderNames;
    }


    /**
     * バインド対象のフォルダ名を渡すとクエリ文字列を返す.<br>
     * 存在しない場合はnullが返る<br>
     *
     * @param folderName
     * @return クエリ文字列
     */
    public String getBindFolderQuery(String folderName) {
        return this.bindQueryFolder.get(folderName);
    }


    /**
     * バインド対象のフォルダ名を渡すとフォルダに指定された主キー項目名のリストを返す.<br>
     * 存在しない場合はnullが返る<br>
     *
     * @param folderName
     * @return 主キー項目名のリスト
     */
    public List<String> getBindFolderPKey(String folderName) {
        return this.queryPKeyColumns.get(folderName);
    }


    /**
     * フォルダ名を指定することでバインド済みのフォルダであるかを返す.<br>
     *
     * @param folderName
     * @return true:存在する、false:存在しない
     */
    public boolean exsisBindFolderName(String folderName) {
        return this.bindQueryFolder.containsKey(folderName);
    }

}
