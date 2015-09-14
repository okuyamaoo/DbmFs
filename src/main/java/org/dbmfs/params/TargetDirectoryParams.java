package org.dbmfs.params;

public class TargetDirectoryParams {

    public String tableName = null;
    public int limit = -1;
    public int offset = -1;

    public TargetDirectoryParams(String tableName) {
        this.tableName = tableName;
    }


    public TargetDirectoryParams(String tableName, int offset, int limit) {
        this.tableName = tableName;
        this.offset = offset;
        this.limit = limit;
    }

    public boolean hasLimitOffsetParams() {
        if (this.limit > 0) return true;
        return false;
    }

    public String toString() {
        return "tableName = " + tableName + ", offset = " + offset + ", limit = " + limit;
    }
}
