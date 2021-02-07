package com.travel.hbase;

import com.travel.common.Constants;
import com.travel.utils.HbaseTools;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class CreateHTabTable {

    public static void main(String[] args) throws IOException {
        Connection hbaseConn = HbaseTools.getHbaseConn();
        String[] tableNames = new String[]{Constants.HTAB_HAIKOU_ORDER, Constants.HTAB_GPS};
        Admin admin = hbaseConn.getAdmin();
        for (String tableName : tableNames) {
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
                // 构造列簇
                ColumnFamilyDescriptorBuilder cdb4f1 = ColumnFamilyDescriptorBuilder.newBuilder(Constants.DEFAULT_FAMILY.getBytes());
                ColumnFamilyDescriptor f1 = cdb4f1.build();
                tdb.setColumnFamily(f1);
                // 创建表
                TableDescriptor td = tdb.build();
                admin.createTable(td);
            }
        }
        admin.close();
        hbaseConn.close();
    }
}
