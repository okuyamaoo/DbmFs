package org.dbmfs;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import fuse.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


/**
 * DBMFS.<br>
 * FUSE-Jインターフェースの実装クラス<br>
 *
 * @author okuyamaoo
 * @license Apache Lisence
 */
public class DatabaseFilesystem implements Filesystem3, XattrSupport {

    private static final Log log = LogFactory.getLog(DatabaseFilesystem.class);

    public volatile static int blockSize = 1024*8;

    private FuseStatfs statfs;

    public static String driverName = null;
    public static String databaseUrl = null;
    public static String databaseAddress = null;
    public static int databasePort = -1;
    public static String databaseName = null;
    public static String user = null;
    public static String password = null;

    private Map openFileStatus = new Hashtable();

    private Object[] syncFileAccess = new Object[10];

    DatabaseClient dbmfsCore = null;



    public DatabaseFilesystem(String driverName, String databaseAddress, int databasePort, String databaseName, String user, String password) throws IOException {
    }

    public DatabaseFilesystem(String driverName, String databaseUrl, String user, String password) throws IOException {
        log.info("database file system mount start ....");

        int files = 0;
        int dirs = 0;
        int blocks = 0;

        DatabaseFilesystem.driverName = driverName;
        DatabaseFilesystem.databaseUrl = databaseUrl;
        DatabaseFilesystem.user = user;
        DatabaseFilesystem.password = password;


        statfs = new FuseStatfs();
        statfs.blocks = Integer.MAX_VALUE;
        statfs.blockSize = 1024 * 14;
        statfs.blocksFree = Integer.MAX_VALUE;
        statfs.files = files + dirs;
        statfs.filesFree = Integer.MAX_VALUE;
        statfs.namelen = 2048;
        try {
            for (int idx = 0; idx < this.syncFileAccess.length; idx++) {
                this.syncFileAccess[idx] = new Object();
            }
            Class.forName(DatabaseFilesystem.driverName);

            dbmfsCore = new DatabaseClient();
        } catch (Exception e) {

            throw new IOException(e);
        }
        log.info("database file system mouted ....");
   }



    public int chmod(String path, int mode) throws FuseException {
        log.info("chmod " + path + " " + mode);
        throw new FuseException("Read Only").initErrno(FuseException.EACCES);
    }

    public int chown(String path, int uid, int gid) throws FuseException {
        log.info("chown " + path + " " + uid + " " + gid);
        throw new FuseException("Read Only").initErrno(FuseException.EACCES);
    }


   public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
        log.info("getattr " + path);
        String[] pathInfo = null;
        String[] setInfo = new String[11];

        try {

            if (path.trim().equals("/")) {

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
                String infomationString = dbmfsCore.getInfomation(path.trim());
//   /a       = dir    1  0  0  0  1435098875  0  493    0  24576974580222
//   /a/1.txt = file  1  0  0  0  1435098888  0  33188  0  24589836752449  -1
//   /a/2.txt = file  1  0  0  0  1435098890  0  33188  0  24591395798630  -1

// File : file  1  0  0  0  1435097370  0  33188  0  23071466454130  -1
// Dir  : dir    1  0  0  0  1435097353  0  493    0  23055035971442
//        0     1 2 3 4 5           6 7     8 9                10
                if (infomationString == null || infomationString.trim().equals("")) return Errno.ENOENT;

                pathInfo = DbmfsUtil.deserializeInfomationString(infomationString);
                if (pathInfo[0].equals("file")) {
                    setInfo[1] = new Integer(FuseFtypeConstants.TYPE_FILE | new Integer(pathInfo[7]).intValue()).toString();
                } else if (pathInfo[0].equals("dir")) {
                    setInfo[1] = new Integer(FuseFtypeConstants.TYPE_DIR | new Integer(pathInfo[7]).intValue()).toString();
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

            Map dirChildMap =  dbmfsCore.getDirectoryInObjects(path.trim());
            if (dirChildMap == null) return Errno.ENOTDIR;

            Set entrySet = dirChildMap.entrySet();
            Iterator entryIte = entrySet.iterator();
            while(entryIte.hasNext()) {

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
    }

    public int mknod(String path, int mode, int rdev) throws FuseException {
        log.info("mknod " + path + " " + mode + " " + rdev);
        throw new FuseException("Read Only").initErrno(FuseException.EACCES);
    }

    public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {
        log.info("open " + path + " " + flags);
        long fileDp = System.nanoTime();

        try {
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
        throw new FuseException("Read Only").initErrno(FuseException.EACCES);
    }

    public int rmdir(String path) throws FuseException {
        log.info("rmdir " + path);
        throw new FuseException("Read Only").initErrno(FuseException.EACCES);
    }

    public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
        log.info("statfs " + statfsSetter);
        statfsSetter.set(blockSize, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
                          0, Integer.MAX_VALUE, 2000);
        return 0;
    }

    public int symlink(String from, String to) throws FuseException {
        log.info("symlink " + from + " " + to);
        return Errno.EACCES;
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
            int readLen = dbmfsCore.readValue(path.trim(), offset, buf.limit(), buf);
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            new FuseException(e);
        }
        return 0;
    }

    public int release(String path, Object fh, int flags) throws FuseException {
        log.info("release " + path + " " + fh +  " " + flags);
        synchronized (this.syncFileAccess[((path.hashCode() << 1) >>> 1) % 10]) {
            openFileStatus.remove(path.trim());
        }
        return 0;
    }

    public int flush(String path, Object fh) throws FuseException {
        log.info("flush " + path + " " + fh);
        synchronized (this.syncFileAccess[((path.hashCode() << 1) >>> 1) % 10]) {
            openFileStatus.remove(path.trim());
        }
        return  0;
   }


    public int fsync(String path, Object fh, boolean isDatasync) throws FuseException {
        log.info("fsync " + path + " " + fh + " " + isDatasync);
        synchronized (this.syncFileAccess[((path.hashCode() << 1) >>> 1) % 10]) {
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
