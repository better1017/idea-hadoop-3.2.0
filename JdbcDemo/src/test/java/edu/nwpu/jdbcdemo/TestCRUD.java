package edu.nwpu.jdbcdemo;

import org.junit.Test;

import java.sql.*;

/**
 * 测试基本操作
 */
public class TestCRUD {
    /**
     * 语句对象：
     * 不能防止sql注入
     * 性能比较低
     */
    @Test
    public void testStatement() throws Exception {
        long start = System.currentTimeMillis();
        // 创建连接
        String jdbcDriver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/big4?serverTimezone=UTC";
        String username = "root";
        String password = "123654";
        Class.forName(jdbcDriver);
        Connection conn = DriverManager.getConnection(url, username, password);
        // 关闭自动提交
        conn.setAutoCommit(false);

        // 创建语句
        Statement st = conn.createStatement();
        for (int i = 0; i < 1000000; i++) {
            String sql = "insert into users(name, age) values('apollo" + i + "'" + ", " + i % 100 + ")";
//            System.out.println(sql);
            st.execute(sql); // 关闭自动提交后，execute语句使得虽然进了数据库，但未提交（如不提交会回滚）
        }
        st.close();
        conn.commit();
        conn.close();
        System.out.println("testStatement: " + (System.currentTimeMillis() - start));
    }

    @Test
    public void testPreparedStatement() throws Exception {
        long start = System.currentTimeMillis();
        // 创建连接
        String jdbcDriver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/big4?serverTimezone=UTC";
        String username = "root";
        String password = "123654";
        Class.forName(jdbcDriver);
        Connection conn = DriverManager.getConnection(url, username, password);
        // 关闭自动提交
        conn.setAutoCommit(false);

        // 创建语句
        String sql = "insert into users(name, age) values(?, ?)";
        PreparedStatement ppst = conn.prepareStatement(sql);
        for (int i = 0; i < 10000000; i++) {
            ppst.setString(1, "Apollo" + i);
            ppst.setInt(2, i % 100);
            ppst.addBatch();
            if (i % 10000 == 0) {
                ppst.executeBatch(); // 未提交
            }
        }
        ppst.executeBatch();
        ppst.close();
        conn.commit();
        conn.close();
        System.out.println("testPreparedStatement: " + (System.currentTimeMillis() - start));
    }

    @Test
    public void testCallableStatement() throws Exception {
        long start = System.currentTimeMillis();
        // 创建连接
        String jdbcDriver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/big4?serverTimezone=UTC";
        String username = "root";
        String password = "123654";
        Class.forName(jdbcDriver);
        Connection conn = DriverManager.getConnection(url, username, password);
        // 关闭自动提交
        conn.setAutoCommit(false);
        // 创建语句
        String sql = "{call sp_batchinsert(?, ?, ?)}"; //SET C = A + B;
        CallableStatement cst = conn.prepareCall(sql);
        // 绑定参数
        cst.setInt(1, 2);
        cst.setInt(2, 3);
        // 注册参数
        cst.registerOutParameter(3, Types.INTEGER);
        // 执行参数
        cst.execute();
        int sum = cst.getInt(3);
        System.out.println("sum = " + sum);
        conn.commit();
        conn.close();
        System.out.println("testCallableStatement: " + (System.currentTimeMillis() - start));
    }

    @Test
    public void testBatchInsert() throws Exception {
        long start = System.currentTimeMillis();
        // 创建连接
        String jdbcDriver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/big4?serverTimezone=UTC";
        String username = "root";
        String password = "123654";
        Class.forName(jdbcDriver);
        Connection conn = DriverManager.getConnection(url, username, password);
        // 关闭自动提交
        conn.setAutoCommit(false);
        // 创建语句
        String sql = "{call sp_batchinsert(?)}"; //批量插入
        CallableStatement cst = conn.prepareCall(sql);
        // 绑定参数
        cst.setInt(1, 10000000);
        // 执行
        cst.execute();
        conn.commit();
        conn.close();
        System.out.println("testBatchInsert: " + (System.currentTimeMillis() - start));
    }

    /**
     * 通过callableStatement调用MySQL函数
     */
    @Test
    public void testFunction() throws Exception {
        long start = System.currentTimeMillis();
        // 创建连接
        String jdbcDriver = "com.mysql.cj.jdbc.Driver";
        String url = "jdbc:mysql://localhost:3306/big4?serverTimezone=UTC";
        String userName = "root";
        String password = "123654";
        Class.forName(jdbcDriver);
        Connection conn = DriverManager.getConnection(url, userName, password);
        // 关闭自动提交
        conn.setAutoCommit(false);
        // 创建可调用语句，调用函数
        String sql = "{? = call sf_add(?, ?)}";
        CallableStatement cst = conn.prepareCall(sql);
        // 注册输出类型
        cst.registerOutParameter(1, Types.INTEGER);
        // 传参数
        cst.setInt(2, 23);
        cst.setInt(3, 32);
        cst.execute();
        conn.commit();
        int sum = cst.getInt(1);
        conn.close();
        System.out.println("sum = " + sum);
        System.out.println("testFunction = " + (System.currentTimeMillis() - start));
    }
}
