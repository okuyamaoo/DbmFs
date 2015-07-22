package org.dbmfs;

import java.util.*;

public class DDLFolder {

    private String metaString = null;
    private Map<String, Map<String, String>> columnMetaMap = new LinkedHashMap();
    private String[] pkeyList = null;


    public DDLFolder(String tableMetaString) {

        metaString = tableMetaString;

        String[] metaStringSplit = tableMetaString.split("____");

        for (int idx = 0; idx < metaStringSplit.length; idx++) {


            if (!metaStringSplit[idx].trim().equals("")) {
                // カラムのメタ情報を分解し作成
                if (metaStringSplit[idx].indexOf("pkey_columns") == 0){
                    // Pkeyの名前配列を作成
                    String pKeyNameMeta = metaStringSplit[idx];
                    String pKeyNames = (pKeyNameMeta.split(":"))[1];
                    pkeyList = pKeyNames.split(",");
                } else {
                    // 各カラム単位のメタ情報を作成
                    String[] columnAllMeta = metaStringSplit[idx].split(",");
                    // カラム名
                    String[] columnName = columnAllMeta[0].split(":");
                    String[] columnType = columnAllMeta[1].split(":");
                    String[] columnSize = columnAllMeta[2].split(":");
                    String[] nullType = columnAllMeta[3].split(":");
                    String[] seqType = columnAllMeta[4].split(":");
                    String[] javaTypeName = columnAllMeta[5].split(":");

                    Map<String, String> metaInfo = new LinkedHashMap();
                    metaInfo.put("column_name", columnName[1]);
                    metaInfo.put("type_name", columnType[1]);
                    metaInfo.put("column_size", null2Blank(columnSize[1]));
                    metaInfo.put("null_type", nullType[1]);
                    metaInfo.put("seq_type", seqType[1]);
                    metaInfo.put("javaTypeName", javaTypeName[1]);
                    columnMetaMap.put(columnName[1], metaInfo);
                }
            }
        }
    }

    private String null2Blank(String target) {
        if (target.trim().equals("null")) return "";
        return target;
    }


    /**
     * テーブルcreate　のSQLを作成
     * TODO:完全にMySQL依存なので、他のDBの場合オーバーライドして作り替える
     */
    public String getCreateSQL(String tableName) {
        StringBuilder strBuf = new StringBuilder();
        strBuf.append("create table ");
        strBuf.append(tableName);
        strBuf.append(" ( ");


        String tableColumnSep = "";
        for (Map<String, String> columnMeta : columnMetaMap.values()) {
            strBuf.append(tableColumnSep);
            strBuf.append(columnMeta.get("column_name"));
            strBuf.append(" ");
            strBuf.append(columnMeta.get("type_name"));
            if (columnMeta.get("type_name").equals("VARCHAR")) {
                String columnSize = columnMeta.get("column_size");
                if (!columnSize.equals("")) {
                    strBuf.append("( ");
                    strBuf.append(columnSize);
                    strBuf.append(") ");
                }
            }
            String nullType = columnMeta.get("null_type");
            if (nullType.trim().equals("NO")) {
                strBuf.append(" NOT NULL ");
            }
            String seqType = columnMeta.get("seq_type");
            if (seqType.trim().equals("YES")) {
                strBuf.append(" AUTO_INCREMENT ");
            }
            tableColumnSep = ",";
        }
        strBuf.append(" , ");
        strBuf.append(" PRIMARY KEY (");
        String pkeySep = "";
        for (String pkeyName : pkeyList) {
            strBuf.append(pkeySep);
            strBuf.append(pkeyName);
            pkeySep = ",";
        }
        strBuf.append(" )");
        strBuf.append(" ) ");
        return strBuf.toString();
    }
}
