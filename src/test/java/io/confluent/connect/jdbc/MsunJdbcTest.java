package io.confluent.connect.jdbc;

import org.junit.Test;
import org.postgresql.util.PGobject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Types;

public class MsunJdbcTest {
    @Test
    public void test() throws Exception {


        // 1. 加载Driver类，Driver类对象将自动被注册到DriverManager类中
        Class.forName("org.postgresql.Driver");

        String url = "jdbc:postgresql://localhost:5432/source";
        // 数据库用户名
        String user = "postgres";
        // 数据库密码
        String password = "postgres";
        // 2. 连接数据库，返回连接对象
        Connection conn = DriverManager.getConnection(url, user, password);


        String sql = "insert into chis.test_data_type(id, name, bit_field, bits_field, varbit_field) values (?,?,?,?,?)";

        PreparedStatement ps = conn.prepareStatement(sql);


        ps.setLong(1, 3L);
        ps.setString(2, "zhansan2");

        PGobject pGobject3 = new PGobject();
        pGobject3.setType("bit");
        pGobject3.setValue("1");
        ps.setObject(3, pGobject3);
        //ps.setObject(4, "111", Types.BIT, 3);
        PGobject pGobject = new PGobject();
        pGobject.setType("bit");
        pGobject.setValue("111");
        ps.setObject(4, pGobject);
        PGobject pGobject2 = new PGobject();
        pGobject2.setType("bit");
        pGobject2.setValue("1111");
        ps.setObject(5, pGobject2);

/*
        ps.setObject(3, 1, Types.BINARY);
        ps.setObject(4, 7, Types.BINARY);
        ps.setObject(5, 7, Types.BINARY);
*/

        ps.executeUpdate();

        ps.close();
        conn.close();


    }
}
