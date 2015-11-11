package org.dbmfs.custom;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import fuse.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.dbmfs.query.*;
import org.dbmfs.params.*;
import org.dbmfs.*;

/**
 * DBMFS.<br>
 * FUSE-Jインターフェースの実装クラス<br>
 * データベースをAPIアクセス可能なものとしてマウントする際のFUSE-J実装クラス
 *
 * @author okuyamaoo
 * @license Apache Lisence
 */
public class ApiFilesystem implements Filesystem3, XattrSupport {

    private static final Log log = LogFactory.getLog(ApiFilesystem.class);

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

    private Map<Object, Map> bufferedSaveData = new HashMap(10);
    private static String bufferedDataBodyKey = "buf";
    private static String bufferedDataOffset = "offset";

    public volatile static String DEFAULT_JSON_ENCODING = "UTF-8";

    private Map openFileStatus = new Hashtable();

    private Object[] syncFileAccess = new Object[10000];


    DatabaseClient dbmfsCore = null;

    public volatile static int DATABASE_TYPE = 1; //1=MySQL, 2=PostgreSQL


    public BindQueryFolder bindQueryFolder = null;


    public ApiFilesystem(String driverName, String databaseAddress, int databasePort, String databaseName, String user, String password) throws IOException {
    }

    public ApiFilesystem(String driverName, String databaseUrl, String user, String password, String readOnly, BindQueryFolder bindQueryFolder) throws IOException {
        log.info("database file system mount start ....");

        int files = 0;
        int dirs = 0;
        int blocks = 0;

        DatabaseFilesystem.driverName = driverName;
        DatabaseFilesystem.databaseUrl = databaseUrl;
        DatabaseFilesystem.user = user;
        DatabaseFilesystem.password = password;
        if (readOnly.equals("1")) readOnlyMount = true;

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
System.out.println( DbmfsUtil.isTableName(path));
                if (infomationString == null || infomationString.trim().equals("")) {
                    // データ無し

                    if (path.indexOf("json") == -1 && DbmfsUtil.countPathSeparator(path) == 1 && DbmfsUtil.isTableName(path)) {

                        // ディレクトリとして結果を返す
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
        throw new FuseException("Read Only").initErrno(FuseException.EACCES);
        //throw new FuseException("Read Only").initErrno(FuseException.EACCES);
    }

    public int mknod(String path, int mode, int rdev) throws FuseException {
        log.info("mknod " + path + " " + mode + " " + rdev);
        throw new FuseException("Read Only").initErrno(FuseException.EACCES);
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
        throw new FuseException("Read Only").initErrno(FuseException.EACCES);
    }

    public int unlink(String path) throws FuseException {
        log.info("unlink " + path);
        throw new FuseException("Read Only").initErrno(FuseException.EACCES);
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
        throw new FuseException("Read Only").initErrno(FuseException.EACCES);
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
        }
        return 0;
    }

    public int flush(String path, Object fh) throws FuseException {
        log.info("flush " + path + " " + fh);
        // ファイルパスから不要なoffset limit指定を取り除く
        path = DbmfsUtil.convertRealPath(path.trim());

        synchronized (this.syncFileAccess[((path.hashCode() << 1) >>> 1) % syncFileAccess.length]) {
            openFileStatus.remove(path.trim());
        }
        return  0;
    }


    public int fsync(String path, Object fh, boolean isDatasync) throws FuseException {
        log.info("fsync " + path + " " + fh + " " + isDatasync);
        // ファイルパスから不要なoffset limit指定を取り除く
        path = DbmfsUtil.convertRealPath(path.trim());

        synchronized (this.syncFileAccess[((path.hashCode() << 1) >>> 1) % syncFileAccess.length]) {
            openFileStatus.remove(path.trim());
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
