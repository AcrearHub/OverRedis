package com.atguigu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseTest {
    public static void main(String[] args) throws IOException {

//        createTable(null,"t1","f1","f2");
//
//        createNamespace(NamespaceDescriptor.create("n1").build());
//
//        modifyTable("t1","f9","f8");
//
//        deleteTable(TableName.valueOf("t1"));

        connection.close();
    }

    //获取Connection对象，且为重量级操作，创建一次给多线程使用，用完关闭
    private static Connection connection ;
    static {
        try {
            Configuration configuration = new Configuration();
            //指定zk
            configuration.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * DDL创建namespace
     */
    public static void createNamespace(NamespaceDescriptor namespaceDescriptor) throws IOException {
        Admin admin = connection.getAdmin();
        //判断namespace是否存在
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespace : namespaceDescriptors) {
            if (namespaceDescriptor.toString().contains(namespace.getName()) || namespaceDescriptor.toString()==null){
                System.out.println("NameSpace:"+namespaceDescriptor+"存在");
                return;
            }
        }
        admin.createNamespace(namespaceDescriptor);
        System.out.println("succeed");
        admin.close();
    }

    /**
     * DDL建表
     */
    public static void createTable(String nsName,String tbName,String... cfs) throws IOException {
        //判断是否有重复的表
        if (tbName==null || "".equals(tbName)){
            System.out.println("表名不能为空");
            return;
        }
        //判断是否有重复的列族
        if (cfs==null || cfs.length==0){
            System.out.println("列族不能为空");
            return;
        }

        //获取Admin对象
        Admin admin = connection.getAdmin();
        //判断namespace是否存在
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor namespaceDescriptor : namespaceDescriptors) {
            if (!namespaceDescriptor.getName().equals(nsName) && !(nsName == null)){
                System.out.println("NameSpace:"+nsName+"不存在");
                return;
            }
        }
        //判断表是否存在
        TableName tableName = TableName.valueOf(nsName, tbName);
        if (admin.tableExists(tableName)){
            System.out.println(((nsName==null || "".equals(nsName))?"default":nsName)+":"+tbName+"表已经存在");
            return;
        }

        //由于无法new TableDescriptor，所以用建造者Builder模式
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        //设置列族
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            ColumnFamilyDescriptor build = columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(build);
        }
        TableDescriptor build = tableDescriptorBuilder.build();
        //建表
        admin.createTable(build);
        System.out.println("succeed");
        admin.close();
    }

    /**
     * DDL改表
     */
    public static void modifyTable(String tbName,String... cfs) throws IOException {
        Admin admin = connection.getAdmin();
        //判断表是否存在
        if (!admin.tableExists(TableName.valueOf(tbName))) {
            System.out.println(tbName + "表不存在");
            return;
        }

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tbName));
        //设置列族
        for (String cf : cfs) {
            ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf));
            ColumnFamilyDescriptor build = columnFamilyDescriptorBuilder.build();
            tableDescriptorBuilder.setColumnFamily(build);
        }
        TableDescriptor build = tableDescriptorBuilder.build();
        admin.modifyTable(build);
        System.out.println("succeed");
        admin.close();
    }

    /**
     * DDL删表
     */
    public static void deleteTable(TableName tableName) throws IOException {
        Admin admin = connection.getAdmin();
        //判断表是否存在
        if (!admin.tableExists(tableName)) {
            System.out.println(tableName + "表不存在");
            return;
        }
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println("succeed");
        admin.close();
    }

    /*
      DML数据语句详见word文档
     */
}
