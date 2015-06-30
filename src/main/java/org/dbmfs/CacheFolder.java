package org.dbmfs;


import java.util.*;
import java.util.concurrent.locks.*;


/**
 * Cacheクラス
 *
 * @author okuyamaoo
 * @license Apache License
 */
public class CacheFolder extends LinkedHashMap {

    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    private static int maxCacheSize = 8192*100;
    private static long cacheExpireTime = 30001L;


    /**
     * コンストラクタ.<br>
     *
     */
    public CacheFolder() {
        super(maxCacheSize, 0.75f, true);
    }



    /**
     * set<br>
     *
     * @param key
     * @param value
     */
    public Object put(Object key, Object value) {
        w.lock();

        try {
            Object[] values = new Object[2];
            values[0] = value;
            values[1] = new Long(System.currentTimeMillis());
            return super.put(key, values);
        } finally {
            w.unlock(); 
        }
    }

    /**
     * get<br>
     *
     * @param key
     * @return Object
     */
    public Object get(Object key) {
        if (!super.containsKey(key)) return null;
        r.lock();
        try { 
            Object[] value = (Object[])super.get(key);

            if (value == null) return null;
            Long cacheTime = (Long)value[1];

            // 10秒経過していたら無効
            if ((System.currentTimeMillis() - cacheTime.longValue()) < cacheExpireTime) {
                return value[0];
            } else {
                super.remove(key);
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally { 
            r.unlock(); 
        }
    }


    /**
     * remove<br>
     *
     * @param key
     * @return Object
     */
    public Object remove(Object key) {
        w.lock();
        try {

            Object ret = super.remove(key);
            return ret;
        } finally {
            w.unlock(); 
        }
    }


    /**
     * containsKey<br>
     *
     * @param key
     * @return boolean
     */
    public boolean containsKey(Object key) {
        r.lock();
        try { 
            Object[] value = (Object[])super.get(key);
            if (value == null) return false;
            Long cacheTime = (Long)value[1];

            // 10秒経過していたら無効
            if ((System.currentTimeMillis() - cacheTime.longValue()) < cacheExpireTime) {
                value[1] = cacheTime + 100L; // 確認された値の有効期限を100ms延長
                return true;
            } else {
                super.remove(key);
                return false;
            }
        } finally { 
            r.unlock(); 
        }
    }

    /**
     * 削除指標実装.<br>
     */
    protected boolean removeEldestEntry(Map.Entry eldest) {

        Object[] value = (Object[])eldest.getValue();

        if (maxCacheSize < super.size()) {
            return true;
        }

        if (value == null) return true;
        Long cacheTime = (Long)value[1];

        // 10秒経過していたら無効
        if ((System.currentTimeMillis() - cacheTime.longValue()) < cacheExpireTime) {
            return false;
        } else {
            return true;
        }
    }
}
