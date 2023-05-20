package com.atguigu;

import java.sql.*;
import java.util.Properties;

public class ThickClientDemo {
    /**
     *JDBC：注册驱动、获取连接、写SQL、编译SQL、设置参数、执行SQL、处理结果、关闭连接
     *
     */
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        deleteData();
    }

    /**
     * 增、改
     */
    public static void addData() throws ClassNotFoundException, SQLException {
        //注册驱动、获取连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        //设置schema到namespace的映射
        Properties properties = new Properties();
        properties.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
        Connection connection = DriverManager.getConnection(url,properties);
        //设置事务
        connection.setAutoCommit(true);
        //写SQL
        String sql = "upsert into student values(?,?,?)";
        //编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //设置参数：给占位符？赋值
        preparedStatement.setString(1,"1005");
        preparedStatement.setString(2,"gouba");
        preparedStatement.setString(3,"shijiazhuang");
        //执行SQL
        preparedStatement.executeUpdate();
        System.out.println("succeed");
        //关闭连接
        preparedStatement.close();
        connection.close();
    }

    /**
     * 删
     */
    public static void deleteData() throws ClassNotFoundException, SQLException {
        //注册驱动、获取连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        //设置schema到namespace的映射
        Properties properties = new Properties();
        properties.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
        Connection connection = DriverManager.getConnection(url,properties);
        //设置事务
        connection.setAutoCommit(true);
        //写SQL
        String sql = "delete from student where id=?";
        //编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //设置参数：给占位符？赋值
        preparedStatement.setString(1,"1001");
        //执行SQL
        preparedStatement.execute();
        System.out.println("succeed");
        //关闭连接
        preparedStatement.close();
        connection.close();
    }

    /**
     * 查
     */
    public static void selectData() throws ClassNotFoundException, SQLException {
        //注册驱动、获取连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        //设置schema到namespace的映射
        Properties properties = new Properties();
        properties.setProperty("phoenix.schema.isNamespaceMappingEnabled","true");
        Connection connection = DriverManager.getConnection(url,properties);
        //写SQL
        String sql = "select id,name,addr from student where id = ?";
        //编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        //设置参数：给占位符？赋值
        preparedStatement.setString(1,"1001");
        //执行SQL
        ResultSet resultSet = preparedStatement.executeQuery();
        if (resultSet.next()){
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            String addr = resultSet.getString("addr");
            //处理结果
            System.out.println(id+":"+name+":"+addr);
        }
        //关闭连接
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
