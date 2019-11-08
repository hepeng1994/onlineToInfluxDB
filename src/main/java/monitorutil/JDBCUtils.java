package monitorutil;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCUtils {
    public void test1(){
        DruidDataSource dataSource = new DruidDataSource();
        //获取驱动
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        //建立连接
        dataSource.setUrl("jdbc:mysql://localhost:3306/class38?serverTimezone=Asia/Shanghai");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        try {
            //获取连接
            DruidPooledConnection conn = dataSource.getConnection();
            PreparedStatement statement = conn.prepareStatement("insert into student values(?,?,?,?)");
            statement.setInt(1, 13);
            statement.setString(2, "小明");
            statement.setString(3, "数据库");
            statement.setInt(4, 150);
            int i = statement.executeUpdate();
            System.out.println(i);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
