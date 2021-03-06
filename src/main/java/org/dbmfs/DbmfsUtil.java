package org.dbmfs;

import java.util.*;
import java.io.*;
import java.lang.reflect.*;
import java.math.BigDecimal;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.dbmfs.params.*;

/**
 * DbmFSでのUtilクラス.＜br＞
 *
 * @author okuyamaoo
 * @license Apache License
 */
public class DbmfsUtil {

    public static String fileNameLastString = ".json";
    public static String pathSeparator = "/";


    /**
     * 指定されたパスがTopディレクトリを表す文字列かを返す
     *
     * @param path ターゲットパス
     * @return true:Topディレクトリ / false:Topディレクトリではない
     */
    public static boolean isTopDirectoryPath(String path) {
        if (path != null && path.trim().equals("/")) return true;
        return false;
    }

    /**
     * ディレクトリのfstatのテンプレート情報を作成し返す
     * @return 情報の1行文字列
     */
    public static String createDirectoryInfoTemplate() {
        StringBuilder strBuf = new StringBuilder();

        strBuf.append("dir");
        strBuf.append("\t");
        strBuf.append("1");
        strBuf.append("\t");
        strBuf.append("0");
        strBuf.append("\t");
        strBuf.append("0");
        strBuf.append("\t");
        strBuf.append("0");
        strBuf.append("\t");
        strBuf.append("1435098875");
        strBuf.append("\t");
        strBuf.append("0");
        strBuf.append("\t");
        strBuf.append("493");
        strBuf.append("\t");
        strBuf.append("0");
        strBuf.append("\t");
        strBuf.append("24576974580222");
        return strBuf.toString();
    }

    /**
     * ファイルのfstatのテンプレート情報を作成し返す
     * @return 情報の1行文字列
     */
    public static String createFileInfoTemplate(int dataSize) {
        StringBuilder strBuf = new StringBuilder();

        strBuf.append("file");
        strBuf.append("\t");
        strBuf.append("1");
        strBuf.append("\t");
        strBuf.append("0");
        strBuf.append("\t");
        strBuf.append("0");
        strBuf.append("\t");
        strBuf.append(dataSize);
        strBuf.append("\t");
        strBuf.append("1435098875");
        strBuf.append("\t");
        strBuf.append("1");
        strBuf.append("\t");
        strBuf.append("33188");
        strBuf.append("\t");
        strBuf.append("0");
        strBuf.append("\t");
        strBuf.append("24576974580222");
        strBuf.append("\t");
        strBuf.append("0");
        return strBuf.toString();
    }


    /**
     * 引数をディレクトリ名だけもしくは、ディレクトリ名とファイル名の配列に分解する
     * key = "/tbl1/Pkey1-AAA.json"<br>
     * 返却値= String[0]:tbl1, String[1]:Pkey1-AAA.json<br>
     *
     * key = "/tbl1"<br>
     * 返却値= String[0]:tbl1<br>
     *
     * @return ディレクトリ名だけもしくは、ディレクトリ名とファイル名の配列
     */
    public static String[] splitTableNameAndPKeyCharacter(String key) {
        String[] splitPath = key.split("/");
        List<String> replaceList = new ArrayList();
        for (int idx = 0; idx < splitPath.length; idx++) {
            if (!splitPath[idx].trim().equals("")) {
                replaceList.add(splitPath[idx]);
            }
        }

        splitPath = replaceList.toArray(new String[0]);
        return splitPath;
    }

    public static String[] deserializeInfomationString(String infomationString) {
        return infomationString.split("\t");
    }

    /**
     * 引数のMapをJson文字列化.<br>
     *
     * @return Jsonフォーマット文字列
     */
    public static String jsonSerialize(Object target)  throws JsonProcessingException {
        return jsonSerialize(target, true);
    }

    public static String jsonSerialize(Object target, boolean format)  throws JsonProcessingException {
        ObjectMapper mapper = null;

        // フォーマット有無
        if (format) {

            mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        } else {
            mapper = new ObjectMapper();
        }

        return mapper.writeValueAsString(target);
    }


    /**
     * 引数のJson文字列をObject化.<br>
     *
     * @param Jsonフォーマット文字列
     * @return 変換後Object
     */
    public static List<Map> jsonDeserialize(String jsonString)  throws IOException, JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonString, List.class);
    }




    /**
     * 引数のJson文字列をObject化.<br>
     *
     * @param Jsonフォーマット文字列
     * @return 変換後Object
     */
    public static Map<String, Object> jsonDeserializeSingleObject(String jsonString)  throws IOException, JsonProcessingException {
        String[] strSplit = jsonString.split("\"__DBMFS_TABLE_META_INFOMATION\"");

        String convertDataString = strSplit[0].trim();
        convertDataString = convertDataString.substring(1, (convertDataString.length() - 1));

        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(convertDataString + "}", Map.class);
    }



    /**
     * fuse-jから渡ってくるパス文字列をdbmfsが理解する適切なファイルパスへ変換する
     * 具体的には/tablename/100_200/file.jsonとなっている部分を/tablename/file.jsonへ置き換える
     * /tablename/100_200や/tablename/100_は/tablenameへ置き換える
     * @param path
     * @return 変換後文字列
     */
    public static String convertRealPath(String path) {
        // Topディレクトリ指定もしくはファイル名が数値のみの場合はそのまま返却
        if (path.equals("/") || path.matches(".*/[0-9]*$")) return path;

        // パスが"/100_"や"/100_200"で終わっている場合
        if (path.matches(".*/[0-9_]*$") || path.matches(".*/[0-9_]*/$")) {

            // 最後の/100_を削除
            String[] pathSplit = path.split("/");
            StringBuilder pathBuf = new StringBuilder();
            pathBuf.append("/");
            pathBuf.append(pathSplit[1]);
            return pathBuf.toString();
        }

        if (path.matches(".*/[0-9_]*$_[0-9_]*$") || path.matches(".*/[0-9_]*$_[0-9_]*/$")) {
            // 最後の/100_を削除
            String[] pathSplit = path.split("/");
            StringBuilder pathBuf = new StringBuilder();
            pathBuf.append("/");
            pathBuf.append(pathSplit[1]);
            return pathBuf.toString();
        }

        // ファイル指定の中にoffset limit の数値が入っているかを確認
        if (path.matches(".*/[0-9_]*/.*json$") == true
            || path.matches(".*/[0-9_]*_[0-9_]*/.*json$") == true) {
            // ファイル指定でかつ offset limitを含む
            // offset limit部分削除
            // Pathは必ず /tablename/100_/filename.json　となっているはず
            String[] pathSplit = path.split("/");
            StringBuilder pathBuf = new StringBuilder();
            for (int idx = 1; idx < pathSplit.length; idx++) {
                if (idx != 2) {
                    pathBuf.append("/");
                    pathBuf.append(pathSplit[idx]);
                }
            }
            return pathBuf.toString();
        }
        return path;
    }


    /**
     * パス指定のディレクトリ文字列からlimit/offset指定が存在する場合はパースしTargetDirectoryParamsクラスオブジェクトを返す
     *
     * @param pathCharacter パス文字列(/dirname/100_　など)
     * @return TargetDirectoryParamsオブジェクト
     */
    public static TargetDirectoryParams parseTargetDirectoryPath(String pathCharacter) {
        if (pathCharacter.trim().equals("") || pathCharacter.trim().equals("/")) return new TargetDirectoryParams("/");

        String[] pathCharacterSplit = pathCharacter.split(pathSeparator);
        if (pathCharacterSplit[pathCharacterSplit.length - 1].matches("[0-9_]*$")) {

            // 数値と"_"のみで構成されているのでlimit. offsetが指定されている可能性有り
            String offsetLimitStr = pathCharacterSplit[pathCharacterSplit.length - 1];

            String[] offsetLimitSplit = offsetLimitStr.split("_");

            int limit = Integer.MAX_VALUE;
            int offset = 0;

            // ファイルパス中の"/100_200"部分を分解 ("100_"などの場合も有り)
            List<String> offsetLimitList = new ArrayList();
            for (String str : offsetLimitSplit) {
                if (!str.trim().equals("")) {
                    offsetLimitList.add(str);
                }
            }


            // テーブル名までのパスを再構築
            StringBuilder targetPath = new StringBuilder();
            for (int idx = 0; idx < pathCharacterSplit.length - 1; idx++) {
                if (!pathCharacterSplit[idx].trim().equals("")) {
                    targetPath.append("/");
                    targetPath.append(pathCharacterSplit[idx]);
                }
            }

            // offset limit の部分がどのように指定されているか確認
            if (offsetLimitList.size() == 1) {

                // offsetのみ指定
                return new TargetDirectoryParams(targetPath.toString(), new Integer(offsetLimitList.get(0)).intValue(), Integer.MAX_VALUE);
            } else if (offsetLimitList.size() == 2) {

                // offset, limit指定
                return new TargetDirectoryParams(targetPath.toString(), new Integer(offsetLimitList.get(0)).intValue(),  (new Integer(offsetLimitList.get(1)).intValue() -  new Integer(offsetLimitList.get(0)).intValue()));
            }

            return new TargetDirectoryParams(pathCharacter);
        } else {

            // テーブル名のみの可能性があるので、そのまま返す
            return new TargetDirectoryParams(pathCharacter);
        }
    }
    /**
     * ファイルのフルパスの文字列を作成し返す.<br>
     * tableName = "tbl1"<br>
     * pKeyConcatStr = "Pkey1-AAA"<br>
     * 返却値= /tbl1/Pkey1-AAA.json<br>
     *
     * @return フルパス文字列
     */
    public static String  createFileFullPathString(String tableName, String pKeyConcatStr) {

        return "/" + tableName + "/" + pKeyConcatStr + DbmfsUtil.fileNameLastString;
    }


    /**
     * ファイル名から規定の拡張子文字列を外した文字列に変換し返す.<br>
     * fileName = "Pkey1-AAA.json"<br>
     * 返却値= Pkey1-AAA<br>
     *
     * @return 拡張子文字列を外した主キーのみの連結文字列
     */
    public static String deletedFileTypeCharacter(String fileName) {
        String[] realPKeysplit = fileName.split(DbmfsUtil.fileNameLastString);
        return realPKeysplit[0];
    }

    public static Map<String, Object> convertJsonMap2TypeMap(Map<String, Object> target, Map<String, Map<String, Object>> meta) throws Exception {

        Map<String, Object> returnMap = new LinkedHashMap();

        for(Map.Entry<String, Object> ent : target.entrySet()) {

            String key = ent.getKey();
            Object value = ent.getValue();

            if (!key.equals(DatabaseAccessor.tableMetaInfoKey) && !key.equals(DatabaseAccessor.tableMetaInfoPKeyKey)) { //テーブルのメタ情報と主キーメタ情報は除外

                Map<String, Object> columnMeta = meta.get(key);
                String javaTypeName = (String)columnMeta.get("javaTypeName");
                returnMap.put(key, deserializeType(value, javaTypeName));
            }
        }
        return returnMap;
    }


    public static int countPathSeparator(String path) {
        if (path == null || path.equals("")) return 0;
        return ((path.length() - path.replaceAll(pathSeparator, "").length()) / pathSeparator.length());
    }


    public static DDLFolder jsonDeserializeDDLObject(String jsonBody) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        List dataMapList =  mapper.readValue(jsonBody, List.class);

        return DDLFolder.createDDLFolder((String)((Map)dataMapList.get(0)).get("__DBMFS_TABLE_META_INFOMATION"));
    }

    /**
     * テーブル名の可能性があるか返す
     * 数値だけや全角などはテーブルとして認めない.<br>
     *
     *
     */
    public static boolean isTableName(String targetName) {

        if (targetName.matches(".*/[0-9]*$") == true) {
            return false;
        } else {
            return true;
        }
     }

    public static boolean isHiddenFile(String targetPath) {
        String[] pathList = splitTableNameAndPKeyCharacter(targetPath);
        if (pathList.length == 1) {

            return false;
        } else if (pathList.length == 2) {

            if (pathList[1].indexOf(".") == 0) {
                // 隠しファイル
                return true;
            }
        }
        return false;
    }

    public static List<String> buildMountTableNames(String tableNames) {
        List<String> mountTableList = new ArrayList();
        String[] tableList = tableNames.split(",");

        for (String str : tableList) {
            if (!str.trim().equals("")) {
                mountTableList.add("/" + str.trim().toLowerCase());
                mountTableList.add("/" + str.trim().toLowerCase() + "/");
            }
        }
        mountTableList.add("/");
        return mountTableList;
    }

    public static Object deserializeType(String value, String javaTypeName) {
        try {
            if (javaTypeName.equals("java.lang.String")) {
                return value;
            } else if (javaTypeName.equals("java.lang.Long")) {
                return new Long(value);
            } else if (javaTypeName.equals("java.lang.Integer")) {
                return new Integer(value);
            } else if (javaTypeName.equals("java.lang.Double")) {
                return new Double(value);
            } else if (javaTypeName.equals("java.lang.Float")) {
                return new Double(value).floatValue();
            } else if (javaTypeName.equals("java.math.BigDecimal")) {
                return new BigDecimal(new Double(value).doubleValue());
            } else if (javaTypeName.equals("java.sql.Date")) {
                return new java.sql.Date((new Long(value)).longValue());
            } else if (javaTypeName.equals("java.sql.Timestamp")) {
                return new java.sql.Timestamp((new Long(value)).longValue());
            } else if (javaTypeName.equals("java.sql.Time")) {
                return new java.sql.Time((new Long(value)).longValue());
            } else if (javaTypeName.equals("java.lang.Boolean")) {
                try {
                    if ((new Integer(value)).intValue() == 1) {
                        return new Boolean("true");
                    } else {
                        return new Boolean("false");
                    }
                } catch (Exception e) {
                    if (value != null) {
                        if (value.trim().toLowerCase().equals("true")) {
                            return new Boolean("true");
                        } else if (value.trim().toLowerCase().equals("false")) {
                            return new Boolean("false");
                        }
                    }
                }
            }
        } catch (Exception ee) {

        }

        return value;
    }

    public static Object deserializeType(Long value, String javaTypeName) {
        if (javaTypeName.equals("java.lang.Long")) {
            return value;
        }

        if (javaTypeName.equals("java.sql.Date")) {
            return new java.sql.Date(value.longValue());
        }


        if (javaTypeName.equals("java.sql.Timestamp")) {
            return new java.sql.Timestamp(value.longValue());
        }


        if (javaTypeName.equals("java.sql.Time")) {
            return new java.sql.Time(value.longValue());
        }

        return value;
    }


    public static Object deserializeType(Integer value, String javaTypeName) {
        if (javaTypeName.equals("java.lang.Integer")) {
            return value;
        }

        if (javaTypeName.equals("java.lang.Boolean")) {
            if (value.intValue() == 1) {
                return new Boolean("true");
            } else {
                return new Boolean("false");
            }
        }
        return value;
    }


    public static Object deserializeType(Double value, String javaTypeName) {
        if (javaTypeName.equals("java.lang.Double")) {
            return value;
        }

        if (javaTypeName.equals("java.lang.Float")) {
            return value.floatValue();
        }

        if (javaTypeName.equals("java.math.BigDecimal")) {
            return new BigDecimal(value.doubleValue());
        }
        return value;
    }


    public static Object deserializeType(byte[] value, String javaTypeName) {
        return value;
    }


    public static Object deserializeType(Boolean value, String javaTypeName) {
        return value;
    }



    public static Object deserializeType(Object value, String javaTypeName) {
        if (value instanceof String) {
            return deserializeType((String)value, javaTypeName);
        } else if (value instanceof Long) {
            return deserializeType((Long)value, javaTypeName);
        } else if (value instanceof Double) {
            return deserializeType((Double)value, javaTypeName);
        } else if (value instanceof Boolean) {
            return deserializeType((Boolean)value, javaTypeName);
        }
        return value;
    }
}
