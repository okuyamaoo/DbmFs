package org.dbmfs;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import fuse.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.dbmfs.query.*;
import org.dbmfs.params.*;

/**
 * DBMFS.<br>
 * FUSE-Jインターフェースの実装クラス<br>
 *
 * @author okuyamaoo
 * @license Apache Lisence
 */
public class DatabaseFilesystem implements Filesystem3, XattrSupport {

    private static final Log log = LogFactory.getLog(DatabaseFilesystem.class);

    public volatile static int blockSize = 1024*64;
    public volatile static boolean useRealSize = true;

    private volatile static boolean readOnlyMount = false;

    private FuseStatfs statfs;

    private volatile static int maxShowFiles = 100; // 1ディレクトリ内に表示する最大ファイル数

    public static String driverName = null;
    public static String databaseUrl = null;
    public static String databaseAddress = null;
    public static int databasePort = -1;
    public static String databaseName = null;
    public static String user = null;
    public static String password = null;
    public static List<String> mountTableNames = null;

    private Map<Object, Map> bufferedSaveData = new HashMap(10);
    private static String bufferedDataBodyKey = "buf";
    private static String bufferedDataOffset = "offset";

    public volatile static String DEFAULT_JSON_ENCODING = "UTF-8";

    private Map openFileStatus = new Hashtable();

    private Object[] syncFileAccess = new Object[10000];




    DatabaseClient dbmfsCore = null;

    public volatile static int DATABASE_TYPE = 1; //1=MySQL, 2=PostgreSQL


    public BindQueryFolder bindQueryFolder = null;


    public DatabaseFilesystem(String driverName, String databaseAddress, int databasePort, String databaseName, String user, String password) throws IOException {
    }

    public DatabaseFilesystem(String driverName, String databaseUrl, String user, String password, String readOnly, String mountTables, BindQueryFolder bindQueryFolder) throws IOException {
        log.info("database file system mount start ....");

        int files = 0;
        int dirs = 0;
        int blocks = 0;

        DatabaseFilesystem.driverName = driverName;
        DatabaseFilesystem.databaseUrl = databaseUrl;
        DatabaseFilesystem.user = user;
        DatabaseFilesystem.password = password;
        if (readOnly.equals("1")) readOnlyMount = true;

        if (mountTables != null && !mountTables.trim().equals("")) {
            // マウント対象のテーブル名が指定されている
            // テーブル名称のリストを作成
            mountTableNames = DbmfsUtil.buildMountTableNames(mountTables);
        }

        this.bindQueryFolder = bindQueryFolder;

        statfs = new FuseStatfs();
        statfs.blocks = Integer.MAX_VALUE;
        statfs.blockSize = 1024 * 64;
        statfs.blocksFree = Integer.MAX_VALUE;
        statfs.files = files + dirs;
        statfs.filesFree = Integer.MAX_VALUE;
        statfs.namelen = 2048;
        try {
            for (int idx = 0; idx < this.syncFileAccess.length; idx++) {
                this.syncFileAccess[idx] = new Object();
            }

            Class.forName(DatabaseFilesystem.driverName);

            if (DatabaseFilesystem.driverName.indexOf("mysql") != -1) {
                DatabaseFilesystem.DATABASE_TYPE = 1;
            } else if (DatabaseFilesystem.driverName.indexOf("postgresql") != -1) {
                DatabaseFilesystem.DATABASE_TYPE = 2;
            }


            DatabaseAccessor.initDatabaseAccessor();

            dbmfsCore = new DatabaseClient(this.bindQueryFolder);
        } catch (Exception e) {

            throw new IOException(e);
        }
        log.info("database file system mouted ....");
   }



    public int chmod(String path, int mode) throws FuseException {
        log.info("chmod " + path + " " + mode);
        throw new FuseException("No Support Method").initErrno(FuseException.EACCES);
    }

    public int chown(String path, int uid, int gid) throws FuseException {
        log.info("chown " + path + " " + uid + " " + gid);
        throw new FuseException("No Support Method").initErrno(FuseException.EACCES);
    }


   public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
        log.info("getattr " + path);

        String[] pathInfo = null;
        String[] setInfo = new String[11];

        try {
            if (DbmfsUtil.isTopDirectoryPath(path)) {

                setInfo[1] = new Integer(FuseFtypeConstants.TYPE_DIR | 0777).toString();
                pathInfo = new String[9];
                pathInfo[1] = "0";
                pathInfo[2] = "0";
                pathInfo[3] = "0";
                pathInfo[4] = "0";
                pathInfo[5] = "0";
                pathInfo[6] = "0";
                pathInfo[8] = "0";
            } else {

                // 表示最大ファイル数を超えた場合のディレクトリの指定か確認

                // パスから不要なoffse limit指定を取り除く
                path = DbmfsUtil.convertRealPath(path.trim());
                String infomationString = dbmfsCore.getInfomation(path);

//   /a       = dir    1  0  0  0  1435098875  0  493    0  24576974580222
//   /a/1.txt = file  1  0  0  0  1435098888  0  33188  0  24589836752449  -1
//   /a/2.txt = file  1  0  0  0  1435098890  0  33188  0  24591395798630  -1

// File : file  1  0  0  0  1435097370  0  33188  0  23071466454130  -1
// Dir  : dir    1  0  0  0  1435097353  0  493    0  23055035971442
//        0     1 2 3 4 5           6 7     8 9                10
                // System.out.println( DbmfsUtil.isTableName(path));
                if (infomationString == null || infomationString.trim().equals("")) {
                    // データ無し

                    if (path.indexOf("json") == -1 && DbmfsUtil.countPathSeparator(path) == 1 && DbmfsUtil.isTableName(path)) {

                        if (dbmfsCore.exsistTmpDir(path)) {

                            // ディレクトリとして結果を返す(mkdirでテーブルを作成された状態)
                            setInfo[1] = new Integer(FuseFtypeConstants.TYPE_DIR | 0777).toString();
                            pathInfo = new String[9];
                            pathInfo[1] = "0";
                            pathInfo[2] = "0";
                            pathInfo[3] = "0";
                            pathInfo[4] = "0";
                            pathInfo[5] = "0";
                            pathInfo[6] = "0";
                            pathInfo[8] = "0";
                        } else {
                            return Errno.ENOENT;
                        }
                    } else {
                        return Errno.ENOENT;
                    }
                } else {
                    // データ有り
                    pathInfo = DbmfsUtil.deserializeInfomationString(infomationString);
                    if (pathInfo[0].equals("file")) {
                        setInfo[1] = new Integer(FuseFtypeConstants.TYPE_FILE | new Integer(pathInfo[7]).intValue()).toString();
                    } else if (pathInfo[0].equals("dir")) {
                        setInfo[1] = new Integer(FuseFtypeConstants.TYPE_DIR | new Integer(pathInfo[7]).intValue()).toString();
                    }
                }
            }
            // データ構造作成
            setInfo[0] = new Integer(path.hashCode()).toString();
            setInfo[2] = pathInfo[1];
            setInfo[3] = pathInfo[2];
            setInfo[4] = pathInfo[3];
            setInfo[5] = pathInfo[8];
            setInfo[6] = pathInfo[4];
            long blockCnt = Long.parseLong(setInfo[6]) / 4096;
            if (Long.parseLong(setInfo[6]) % 4096 > 0) blockCnt++;
            setInfo[8] = pathInfo[5];
            setInfo[9] = pathInfo[5];
            setInfo[10] = pathInfo[5];

            getattrSetter.set(new Long(setInfo[0]).longValue(),
                              new Integer(setInfo[1]).intValue(),
                              new Integer(setInfo[2]).intValue(),
                              new Integer(setInfo[3]).intValue(),
                              new Integer(setInfo[4]).intValue(),
                              new Integer(setInfo[5]).intValue(),
                              new Long(setInfo[6]).longValue(),
                              blockCnt,
                              new Integer(setInfo[8]).intValue(),
                              new Integer(setInfo[9]).intValue(),
                              new Integer(setInfo[10]).intValue());

        } catch (FuseException fe) {
            fe.printStackTrace();
            throw fe;
        } catch (Exception e) {
            e.printStackTrace();
            new FuseException(e).initErrno(FuseException.EACCES);
        }
        return 0;
   }

    public int getdir(String path, FuseDirFiller dirFiller) throws FuseException {
        log.info("getdir " + path);

        try {

            // /a = {/a/1.txt=file, /a/2.txt=file}
            // / = {/a=dir, /3.txt=file}

            // path指定から取得するテーブル及び取得位置を特定
            // DbmfsUtil.parseLimitOffsetCharacter(); TODO:ここでテーブル名部分とlimit offset指定がある場合はバラす
            TargetDirectoryParams targetDirectoryParams = DbmfsUtil.parseTargetDirectoryPath(path);

            Map dirChildMap =  null;
            if (targetDirectoryParams.hasLimitOffsetParams()) {
                dirChildMap =  dbmfsCore.getDirectoryInObjects(targetDirectoryParams.tableName, targetDirectoryParams.offset, targetDirectoryParams.limit);
            } else {
                dirChildMap =  dbmfsCore.getDirectoryInObjects(targetDirectoryParams.tableName);
            }
            if (dirChildMap == null) return Errno.ENOTDIR;

            Set entrySet = dirChildMap.entrySet();
            Iterator entryIte = entrySet.iterator();

            // 暫定的に100件を超えるレコードの場合は"100_"というフォルダを仮想的に作成
            int nowCount = 0;
            while(entryIte.hasNext()) {

                nowCount++;
                // offset limit指定を含まずに表示最大数を超えたらbreak;
                if (maxShowFiles < nowCount &&
                    targetDirectoryParams.hasLimitOffsetParams() == false) break;

                Map.Entry obj = (Map.Entry)entryIte.next();

                String name = (String)obj.getKey();
                String objType = (String)obj.getValue();
                String[] nameCnv = name.split("/");

                if (objType.equals("file")) {
                    dirFiller.add(nameCnv[nameCnv.length - 1], 0L, FuseFtype.TYPE_FILE);
                } else if(objType.equals("dir")) {
                    dirFiller.add(nameCnv[nameCnv.length - 1], 0L, FuseFtype.TYPE_DIR);
                }
            }
            // 仮想的に"100_"フォルダを作成
            if (dirChildMap.size() > maxShowFiles && targetDirectoryParams.hasLimitOffsetParams() == false) dirFiller.add(maxShowFiles + "_", 0L, FuseFtype.TYPE_DIR);
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            throw new FuseException(e);
        }
        return 0;
    }

    public int link(String from, String to) throws FuseException {
        log.info("link " + from + " " + to);
        return Errno.EACCES;
    }

    public int mkdir(String path, int mode) throws FuseException {
        log.info("mkdir " + path + " " + mode);
        if (readOnlyMount) throw new FuseException("Read Only").initErrno(FuseException.EACCES);
        dbmfsCore.createTmpDir(path.trim());
        return 0;
        //throw new FuseException("Read Only").initErrno(FuseException.EACCES);
    }

    public int mknod(String path, int mode, int rdev) throws FuseException {
        log.info("mknod " + path + " " + mode + " " + rdev);
        if (readOnlyMount) throw new FuseException("Read Only").initErrno(FuseException.EACCES);

        // ファイルパスが"."で始まる隠しファイルは成功した様に振る舞う
        if (DbmfsUtil.isHiddenFile(path)) {
            //System.out.println(path + " is hidden file");
            return 0;
        }

        // ファイルパスから不要なoffset limit指定を取り除く
        path = DbmfsUtil.convertRealPath(path.trim());

        String modeStr = Integer.toOctalString(mode);
        String pathType = "";
        String fileBlockIdx = null;
        if (modeStr.indexOf("100") == 0) {

            // Regular File
            pathType = "file";
            fileBlockIdx = "-1";
        } else if (modeStr.indexOf("40") == 0) {

            // Directory
            pathType = "dir";
            throw new FuseException("Directory not created").initErrno(FuseException.EACCES);
        } else {

            return Errno.EINVAL;
        }

        StringBuilder infomationBuf = new StringBuilder();
        infomationBuf.append(pathType);
        infomationBuf.append("\t").append("1");
        infomationBuf.append("\t").append("0");
        infomationBuf.append("\t").append("0");
        infomationBuf.append("\t").append("0");
        infomationBuf.append("\t").append((System.currentTimeMillis() / 1000L));
        infomationBuf.append("\t").append("0");
        infomationBuf.append("\t").append(mode);
        infomationBuf.append("\t").append(rdev);
        infomationBuf.append("\t").append(System.nanoTime());
        if (fileBlockIdx != null) {
            infomationBuf.append("\t").append(fileBlockIdx);
        }

        try {
            String checkInfomation = dbmfsCore.getInfomation(path);

            if (checkInfomation != null && !checkInfomation.trim().equals("")) return Errno.EEXIST;
            if (!dbmfsCore.createTmpiNode(path.trim(), infomationBuf.toString())) return Errno.EEXIST;

        } catch (FuseException fe) {

            throw fe;
        } catch (Exception e) {

            new FuseException(e);
        }

        return 0;
    }

    public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
        log.info("open " + path + " " + flags);
        long fileDp = System.nanoTime();

        try {
            // ファイルパスから不要なoffset limit指定を取り除く
            path = DbmfsUtil.convertRealPath(path.trim());

            String pathInfo = dbmfsCore.getInfomation(path.trim());
            if (pathInfo == null || pathInfo.trim().equals("")) return Errno.ENOENT;

            Map<String, Object> openDt = new HashMap();

            openDt.put("filedp", fileDp);
            openDt.put("pathInfoStr", pathInfo);
            openFileStatus.put(path.trim(), openDt);

            openSetter.setFh(openDt);
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            e.printStackTrace();
            new FuseException(e);
        }
        return 0;
    }

    public int rename(String from, String to) throws FuseException {
        log.info("rename " + from + " " + to);
        throw new FuseException("No Support Method").initErrno(FuseException.EACCES);
    }

    public int rmdir(String path) throws FuseException {
        log.info("rmdir " + path);
        throw new FuseException("No Support Method").initErrno(FuseException.EACCES);
    }

    public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
        log.info("statfs " + statfsSetter);
        statfsSetter.set(blockSize, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
                          0, Integer.MAX_VALUE, 2000);
        return 0;
    }

    public int symlink(String from, String to) throws FuseException {
        log.info("symlink " + from + " " + to);
        throw new FuseException("No Support Method").initErrno(FuseException.EACCES);
    }

    public int truncate(String path, long size) throws FuseException {
        log.info("truncate " + path + " " + size);
        if (readOnlyMount) throw new FuseException("Read Only").initErrno(FuseException.EACCES);

        //throw new FuseException("Read Only").initErrno(FuseException.EACCES);
        try {
            //dbmfsCore.deleteData(path, null);
        /*} catch (FuseException fe) {
            throw fe;*/
        } catch (Exception e) {
            new FuseException(e);
        }
        return 0;
    }

    public int unlink(String path) throws FuseException {
        log.info("unlink " + path);
        if (readOnlyMount) throw new FuseException("Read Only").initErrno(FuseException.EACCES);
        //throw new FuseException("Read Only").initErrno(FuseException.EACCES);
        try {
            // ファイルパスから不要なoffset limit指定を取り除く
            path = DbmfsUtil.convertRealPath(path.trim());

            dbmfsCore.deleteData(path);
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            new FuseException(e);
        }
        return 0;
    }

    public int utime(String path, int atime, int mtime) throws FuseException {
        log.info("utime " + path + " " + atime + " " + mtime);
        return 0;
    }

    public int readlink(String path, CharBuffer link) throws FuseException {
        log.info("readlink " + path);
        link.append(path);
        return 0;
    }

    public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {
        log.info("write  path:" + path + " offset:" + offset + " isWritepage:" + isWritepage + " buf.limit:" + buf.limit());
        if (readOnlyMount) throw new FuseException("Read Only").initErrno(FuseException.EACCES);
        try {

            if (fh == null) return Errno.EBADE;

            // ファイルパスから不要なoffset limit指定を取り除く
            path = DbmfsUtil.convertRealPath(path.trim());

            synchronized (syncFileAccess[((path.hashCode() << 1) >>> 1) % syncFileAccess.length]) {

                if (bufferedSaveData.containsKey(fh)) {

                    Map bufferedData = bufferedSaveData.get(fh);
                    ByteArrayOutputStream bufferedByteData = (ByteArrayOutputStream)bufferedData.get(bufferedDataBodyKey);
                    long bOffset = ((Long)bufferedData.get(bufferedDataOffset)).longValue();

                    if ((bOffset + bufferedByteData.size()) == offset) {

                        byte[] nowWriteBytes = new byte[buf.limit()];
                        buf.get(nowWriteBytes);
                        bufferedByteData.write(nowWriteBytes);

                        return 0;
                    }
                } else {

                    Map bufferedData = new HashMap();

                    bufferedData.put("path", path);
                    bufferedData.put("fh", fh);
                    bufferedData.put("isWritepage", isWritepage);

                    ByteArrayOutputStream bufferedByteData = new ByteArrayOutputStream(1024*1024*2);
                    byte[] nowWriteBytes = new byte[buf.limit()];
                    buf.get(nowWriteBytes);

                    bufferedByteData.write(nowWriteBytes);
                    bufferedData.put(bufferedDataBodyKey, bufferedByteData);
                    bufferedData.put(bufferedDataOffset, offset);

                    this.bufferedSaveData.put(fh, bufferedData);
                    return 0;
                }
            }
        } catch (Exception e) {

            throw new FuseException(e);
        }
        return 0;
    }


    public int saveData(String path, Object fh, boolean isWritepage,  byte[] writeData, long offset) throws FuseException {
        log.info("saveData  path:" + path + " offset:" + offset + " isWritepage:" + isWritepage + " buf.limit:" + writeData.length);

        if (fh == null) return Errno.EBADE;

        try {

            if (writeData == null || writeData.length < 1) {
                return Errno.EBADE;
            } else {

                if (!dbmfsCore.saveData(path.trim(), new String(writeData, DEFAULT_JSON_ENCODING))) {
                    return Errno.EBADE;
                }
            }
        } catch (FuseException fe) {
            throw fe;
        }catch (Exception e) {
            new FuseException(e);
        }
        return 0;
    }

    public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException {
        log.info("read:" + path + " offset:" + offset + " buf.limit:" + buf.limit());
        if (fh == null) return Errno.EBADE;
        try {
            // ファイルパスから不要なoffset limit指定を取り除く
            path = DbmfsUtil.convertRealPath(path.trim());

            int readLen = dbmfsCore.readValue(path, offset, buf.limit(), buf);
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            new FuseException(e);
        }
        return 0;
    }

    public int release(String path, Object fh, int flags) throws FuseException {
        log.info("release " + path + " " + fh +  " " + flags);
        // ファイルパスから不要なoffset limit指定を取り除く
        path = DbmfsUtil.convertRealPath(path.trim());

        synchronized (this.syncFileAccess[((path.hashCode() << 1) >>> 1) % syncFileAccess.length]) {

            openFileStatus.remove(path.trim());
            saveBufferedData(fh);
        }
        return 0;
    }

    public int flush(String path, Object fh) throws FuseException {
        log.info("flush " + path + " " + fh);
        // ファイルパスから不要なoffset limit指定を取り除く
        path = DbmfsUtil.convertRealPath(path.trim());

        synchronized (this.syncFileAccess[((path.hashCode() << 1) >>> 1) % syncFileAccess.length]) {
            openFileStatus.remove(path.trim());
            saveBufferedData(fh);
        }
        return  0;
    }


    public int fsync(String path, Object fh, boolean isDatasync) throws FuseException {
        log.info("fsync " + path + " " + fh + " " + isDatasync);
        // ファイルパスから不要なoffset limit指定を取り除く
        path = DbmfsUtil.convertRealPath(path.trim());

        synchronized (this.syncFileAccess[((path.hashCode() << 1) >>> 1) % syncFileAccess.length]) {
            openFileStatus.remove(path.trim());
            saveBufferedData(fh);
        }
        return 0;
    }

    private int saveBufferedData(Object fh) throws FuseException {
        log.info("saveBufferedData " + fh);

        if (bufferedSaveData.containsKey(fh)) {
            Map bufferedData = bufferedSaveData.remove(fh);
            if (bufferedData != null) {
                String path = (String)bufferedData.get("path");
                Object bufferedFh = (Object)bufferedData.get("fh");
                boolean isWritepage = ((Boolean)bufferedData.get("isWritepage")).booleanValue();
                ByteArrayOutputStream buf = (ByteArrayOutputStream)bufferedData.get(bufferedDataBodyKey);
                long offset = ((Long)bufferedData.get(bufferedDataOffset)).longValue();

                int realWriteRet = saveData(path, bufferedFh, isWritepage, buf.toByteArray(), offset);
            }
        }
        return 0;
    }


    public int getxattr(String path, String name, ByteBuffer dst) throws FuseException, BufferOverflowException {
       return 0;
    }

        public int setxattr(String path, String name, ByteBuffer value, int flags) throws FuseException {
       return 0;
    }

    public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter) throws FuseException {
       return 0;
    }

    public int listxattr(String path, XattrLister lister) throws FuseException {
       return 0;
    }

    public int removexattr(String path, String name) throws FuseException {
       return 0;
    }

}
