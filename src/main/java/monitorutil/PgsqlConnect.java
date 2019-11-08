
package monitorutil;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


/**
 *     获取Connection对象
 *     方案一 ：简单版,直接把配置信息写在代码中
 *
 *
 */


public class PgsqlConnect {
    public static String url;
    public static  String username ;
    public static  String password;
    static {
        try {
            url = new PropertiesUtil().readValue("pgsql_url");
            username = new PropertiesUtil().readValue("pgsql_user");
            password = new PropertiesUtil().readValue("pgsql_password");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static Connection getConnection() {
//        1.注册驱动
        try {
            Class.forName("org.postgresql.Driver");

//        2.获取连接
            System.out.println(url);
            System.out.println(username);
            System.out.println(password);
            Connection conn = DriverManager.getConnection(url,username,password);
//        3.    返回连接对象
            System.out.println(conn);
            return conn;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    public static void release(Connection conn,Statement stat,ResultSet rs) {
        if(rs!=null) {
            try {
                rs.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }finally {

                if(stat!=null) {
                    try {
                        stat.close();
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }finally {

                        if(conn!=null) {
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
    }
}