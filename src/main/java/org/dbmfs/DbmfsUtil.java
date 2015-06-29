package org.dbmfs;

import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.SerializationFeature;

/**
 * DbmFSでのUtilクラス.＜br＞
 *
 * @author okuyamaoo
 * @license Apache License
 */
public class DbmfsUtil {

		public static String fileNameLastString = ".json";

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
		public static String jsonSerialize(List<Map<String, Object>> target)  throws JsonProcessingException {
				return jsonSerialize(target, true);
		}

		public static String jsonSerialize(List<Map<String, Object>> target, boolean format)  throws JsonProcessingException {
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
		 * ファイルのフルパスの文字列を作成し返す.<br>
		 * tableName = "tbl1"<br>
		 * pKeyConcatStr = "Pkey1-AAA"<br>
		 * 返却値= /tbl1/Pkey1-AAA.json<br>
		 *
		 * @return フルパス文字列
		 */
		public static String	createFileFullPathString(String tableName, String pKeyConcatStr) {

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
}
