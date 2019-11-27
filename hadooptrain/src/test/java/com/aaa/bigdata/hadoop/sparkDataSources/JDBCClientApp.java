package com.aaa.bigdata.hadoop.sparkDataSources;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.sql.*;


/**
 * description: 描述
 *
 * @author 田留振(liuzhen.tian @ haoxiaec.com)
 * @version 1.0
 * @date 2019/11/27
 */
public class JDBCClientApp {
    public static void main(String[] args) throws Exception {
        // 加载驱动
        Class.forName("com.mysql.jdbc.Driver");
        //加载src/main/resources目录下的application.conf文件
        Config config = ConfigFactory.load();
        String url = config.getString("db.default.url");
        String user = config.getString("db.default.user");
        String password = config.getString("db.default.password");
        Connection conn = DriverManager.getConnection(url,user,password);
        PreparedStatement ps = conn.prepareStatement("select * from test");
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            System.out.println(rs.getObject(1) + " : " + rs.getObject(2));
        }
        rs.close();
        ps.close();
        conn.close();
    }
}
