package org.dbmfs.ddl;

import java.util.*;

import org.dbmfs.DDLFolder;

public class PostgreSQLDDLFolder extends DDLFolder {

    public PostgreSQLDDLFolder(String tableMetaString) {
        super(tableMetaString);
    }

    /**
     * テーブルcreate　のSQLを作成
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

            String typeName = columnMeta.get("type_name");


            if (typeName.equals("DATETIME")) {
                strBuf.append("TIMESTAMP");
            } else if (typeName.equals("BLOB")) {
                strBuf.append("BYTEA");
            } else {
                strBuf.append(typeName);

            }
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

                strBuf.append(" SERIAL ");
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
