package com.github.bingoohuang.mtcp.db;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.pool.TestElf;
import lombok.val;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.SQLException;

public class BasicTest {
    @Test @Ignore
    public void test() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinIdle(1);
        config.setMaxPoolSize(2);
        config.setConnectionTestQuery("SELECT 1");
        config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/?useUnicode=true&&characterEncoding=UTF-8&connectTimeout=30000&socketTimeout=30000&autoReconnect=true");
        config.setUsername("user");
        config.setPassword("pass");

        try (val ds = new LightDataSource(config);
             val conn = ds.getConnection();
             val stmt = conn.createStatement()) {
            stmt.executeUpdate("DROP TABLE IF EXISTS basic_pool_test");
        }
    }

}
