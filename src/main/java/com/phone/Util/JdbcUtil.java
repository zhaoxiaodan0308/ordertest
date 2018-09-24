package com.phone.Util;

import com.phone.common.GlobalConstants;

import java.sql.*;

/**
 * FileName: JdbcUtil
 * Author: zhao
 * Date: 2018/9/20 20:46
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class JdbcUtil {

    //静态加载驱动
    static {
        try {
            Class.forName(GlobalConstants.DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接
     * @return
     */
    public static Connection getConn(){
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(GlobalConstants.URL,GlobalConstants.USER
                    ,GlobalConstants.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 关闭mysql的相关对象
     * @param conn
     * @param ps
     * @param rs
     */
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs){
        if(conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                //do nothing
            }
        }

        if(ps != null){
            try {
                ps.close();
            } catch (SQLException e) {
                //do nothing
            }
        }

        if(rs != null){
            try {
                rs.close();
            } catch (SQLException e) {
                //do nothing
            }
        }
    }

    public static void main(String[] args) {
        System.out.println(JdbcUtil.getConn());
    }
}
