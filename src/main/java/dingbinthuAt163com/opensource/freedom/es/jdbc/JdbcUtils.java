package dingbinthuAt163com.opensource.freedom.es.jdbc;


import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *@author DingBin,  dingbinthu@163.com
 *@create 2017-07-08, 8:19
 */
public class JdbcUtils {

    private static Map<String,DataSource> dsMap;
    static {
        dsMap = new HashMap<String,DataSource>();
        dsMap.put("default",new ComboPooledDataSource());//直接使用即可，不用显示的配置，其会自动识别配置文件
        dsMap.put("myApp",new ComboPooledDataSource("myApp"));//直接使用即可，不用显示的配置，其会自动识别配置文件
    }

    public static DataSource getDataSource(String name) {
        return dsMap.get(name);
    }

    public static Connection getConnection(String name) throws SQLException {
        try {
            return getDataSource(name).getConnection();
        } catch (SQLException ex) {
            throw ex;
        }
    }

    public static void closeConnection(Connection conn){
        if (null != conn) {
            try {
                conn.close();
            }
            catch (Exception exe) {
                exe.printStackTrace();
            }
        }
    }

    /**
     * 增加、删除、改
     *
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public static boolean updateByPreparedStatement(Connection conn, String sql, List<Object> params) throws SQLException {
        boolean flag = false;
        int result = -1;
        PreparedStatement pstmt = conn.prepareStatement(sql);
        int index = 1;
        if (params != null && !params.isEmpty()) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(index++, params.get(i));
            }
        }
        result = pstmt.executeUpdate();
        flag = result > 0 ? true : false;
        return flag;
    }


    /**
     * 增加、删除、改
     *
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public static boolean updateByPreparedStatement(String dbConfigName, String sql, List<Object> params) throws SQLException {
        Connection conn = getConnection(dbConfigName);
        boolean ret = updateByPreparedStatement(conn,sql,params);
        closeConnection(conn);
        return ret;
    }

    /**
     * 查询单条记录
     *
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public static Map<String, Object> findSimpleResult(Connection conn, String sql, List<Object> params) throws SQLException {
        Map<String, Object> map = new HashMap<String, Object>();
        int index = 1;
        PreparedStatement pstmt = conn.prepareStatement(sql);
        if (params != null && !params.isEmpty()) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(index++, params.get(i));
            }
        }
        ResultSet resultSet = pstmt.executeQuery();//返回查询结果
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int col_len = metaData.getColumnCount();
            while (resultSet.next()) {
                for (int i = 0; i < col_len; i++) {
                    String cols_name = metaData.getColumnLabel(i + 1);
                    Object cols_value = resultSet.getObject(cols_name);
                    if (cols_value == null) {
                        cols_value = "";
                    }
                    map.put(cols_name, cols_value);
                }
            }
            resultSet.close();
        }
        catch (SQLException e) {
            resultSet.close();
            throw e;
        }
        return map;
    }


    /**
     * 查询单条记录
     *
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public static Map<String, Object> findSimpleResult(String dbConfigName, String sql, List<Object> params) throws SQLException {
        Connection conn = getConnection(dbConfigName);
        Map<String,Object> map = findSimpleResult(conn,sql,params);
        closeConnection(conn);
        return map;
    }

    /**
     * 查询多条记录
     *
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public static List<Map<String, Object>> findModeResult(Connection conn,String sql, List<Object> params) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        int index = 1;
        PreparedStatement pstmt = conn.prepareStatement(sql);
        if (params != null && !params.isEmpty()) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(index++, params.get(i));
            }
        }
        ResultSet resultSet = pstmt.executeQuery();
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int cols_len = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (int i = 0; i < cols_len; i++) {
                    String cols_name = metaData.getColumnLabel(i + 1);
                    Object cols_value = resultSet.getObject(cols_name);
                    if (cols_value == null) {
                        cols_value = "";
                    }
                    map.put(cols_name, cols_value);
                }
                list.add(map);
            }
            resultSet.close();
        }
        catch ( SQLException e) {
            resultSet.close();
            throw e;
        }
        return list;
    }


    /**
     * 查询多条记录
     *
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    public static List<Map<String, Object>> findModeResult(String dbConfigName, String sql, List<Object> params) throws SQLException {
        Connection conn = getConnection(dbConfigName);
        List<Map<String,Object>> list = findModeResult(conn,sql,params);
        closeConnection(conn);
        return list;
    }

    /**
     * 通过反射机制查询单条记录
     *
     * @param sql
     * @param params
     * @param cls
     * @return
     * @throws Exception
     */
    public static <T> T findSimpleRefResult(Connection conn, String sql, List<Object> params, Class<T> cls) throws Exception {
        T resultObject = null;

        int index = 1;
        PreparedStatement pstmt = conn.prepareStatement(sql);
        if (params != null && !params.isEmpty()) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(index++, params.get(i));
            }
        }
        ResultSet resultSet = pstmt.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols_len = metaData.getColumnCount();
        while (resultSet.next()) {
            //通过反射机制创建一个实例
            resultObject = cls.newInstance();
            for (int i = 0; i < cols_len; i++) {
                String cols_name = metaData.getColumnLabel(i + 1);
                Object cols_value = resultSet.getObject(cols_name);
                if (cols_value == null) {
                    cols_value = "";
                }
                Field field = cls.getDeclaredField(cols_name);
                field.setAccessible(true); //打开javabean的访问权限
                field.set(resultObject, cols_value);
            }
        }
        return resultObject;

    }


    /**
     * 通过反射机制查询单条记录
     *
     * @param sql
     * @param params
     * @param cls
     * @return
     * @throws Exception
     */
    public static <T> T findSimpleRefResult(String dbConfigName, String sql, List<Object> params, Class<T> cls) throws Exception {
        Connection conn = getConnection(dbConfigName);
        T t = findSimpleRefResult(conn,sql,params,cls);
        closeConnection(conn);
        return t;
    }

    /**
     * 通过反射机制查询多条记录
     *
     * @param sql
     * @param params
     * @param cls
     * @return
     * @throws Exception
     */
    public static <T> List<T> findMoreRefResult(Connection conn, String sql, List<Object> params, Class<T> cls) throws Exception {
        List<T> list = new ArrayList<T>();
        int index = 1;
        PreparedStatement pstmt = conn.prepareStatement(sql);
        if (params != null && !params.isEmpty()) {
            for (int i = 0; i < params.size(); i++) {
                pstmt.setObject(index++, params.get(i));
            }
        }
        ResultSet resultSet = pstmt.executeQuery();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int cols_len = metaData.getColumnCount();
        while (resultSet.next()) {
            //通过反射机制创建一个实例
            T resultObject = cls.newInstance();
            for (int i = 0; i < cols_len; i++) {
                String cols_name = metaData.getColumnLabel(i + 1);
                Object cols_value = resultSet.getObject(cols_name);
                if (cols_value == null) {
                    cols_value = "";
                }
                Field field = cls.getDeclaredField(cols_name);
                field.setAccessible(true); //打开javabean的访问权限
                field.set(resultObject, cols_value);
            }
            list.add(resultObject);
        }
        return list;
    }

    /**
     * 通过反射机制查询多条记录
     *
     * @param sql
     * @param params
     * @param cls
     * @return
     * @throws Exception
     */
    public static <T> List<T> findMoreRefResult(String dbConfigName, String sql, List<Object> params, Class<T> cls) throws Exception {
        Connection conn = getConnection(dbConfigName);
        List<T> list = findMoreRefResult(conn,sql,params,cls);
        closeConnection(conn);
        return list;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws SQLException {

        String dbConfigName = "myApp";
        /*******************建表*********************/
        /*
        create table `user_info` (
        `id` int(30) NOT NULL AUTO_INCREMENT COMMENT '编号',
        `username`  varchar(100) NOT NULL COMMENT '姓名',
        `password`  varchar(100)  NOT NULL COMMENT '密码',
                PRIMARY KEY (`id`)
        ) Engine = INNODB DEFAULT CHARSET=utf8 COMMENT='学生信息表'
        */

        /*******************增*********************/
        String sql = "insert into user_info (username, password) values (?, ?), (?, ?), (?, ?)";
        List<Object> params = new ArrayList<Object>();
        params.add("小明");
        params.add("123xiaoming");
        params.add("张三");
        params.add("zhangsan");
        params.add("李四");
        params.add("lisi000");
        try {
            boolean flag = JdbcUtils.updateByPreparedStatement(dbConfigName,sql, params);
            System.out.println(flag ? "SUCCESS" : "FAILED");
        } catch (SQLException e) {
            e.printStackTrace();
        }


        /*******************删*********************/
        //删除名字为张三的记录
        String sql1 = "delete from user_info where username = ?";
        List<Object> params1 = new ArrayList<Object>();
        params1.add("小明");
        try {
            boolean flag = JdbcUtils.updateByPreparedStatement(dbConfigName, sql1, params1);
            System.out.println(flag ? "SUCCESS" : "FAILED");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        /*******************改*********************/
        //将名字为李四的密码改了
        String sql2 = "update user_info set password = ? where username = ? ";
        List<Object> params2 = new ArrayList<Object>();
        params2.add("lisi88888");
        params2.add("李四");
        try {
            boolean flag = JdbcUtils.updateByPreparedStatement(dbConfigName, sql2, params2);
            System.out.println(flag ? "SUCCESS" : "FAILED");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        /*******************查*********************/
        //不利用反射查询多个记录
        String sql3 = "select * from user_info ";
        try {
            List<Map<String, Object>> list = JdbcUtils.findModeResult(dbConfigName,sql3, null);
            System.out.println(list);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //利用反射查询 单条记录
        String sql4 = "select * from user_info where username = ? ";
        List<Object> params4 = new ArrayList<Object>();
        params4.add("李四");
        UserInfo userInfo;
        try {
            userInfo = JdbcUtils.findSimpleRefResult(dbConfigName,sql4, params4, UserInfo.class);
            System.out.print(userInfo);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}