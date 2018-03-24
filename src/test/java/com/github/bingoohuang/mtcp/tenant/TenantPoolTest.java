package com.github.bingoohuang.mtcp.tenant;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.pool.TestElf;
import lombok.val;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

public class TenantPoolTest {
    static LightDataSource ds;

    @BeforeClass
    public static void beforeClass() throws SQLException {
        val config = createH2LightConfig();
        config.setTenantEnvironmentAwareClassName(MyTenantEnvironment.class.getName());
        ds = new LightDataSource(config);

        try (val conn = ds.getConnection();
             val stmt = conn.createStatement()) {
            stmt.executeUpdate("CREATE TABLE T_CURRENT_TENANT(TENANT_ID VARCHAR(128))");
        }
    }

    @AfterClass
    public static void afterClass() {
        ds.close();
    }

    @Test
    public void test1() throws SQLException {
        checkTenant("XXX");
        checkTenant("YYY");
    }

    public static final int ITERATIONS = 10_000;

    @Test
    public void test2() {
        val threadPool = Executors.newFixedThreadPool(10);
        for (int i = 0; i < ITERATIONS; i++) {
            val tenantId = "TID" + i;
            threadPool.submit((Callable<String>) () -> {
                checkTenant(tenantId);
                return null;
            });
        }
    }

    private void checkTenant(String tenantId) throws SQLException {
        MyTenantContext.setTenantId(tenantId);
        try (val conn = ds.getConnection();
             val stmt = conn.createStatement();
             val rs = stmt.executeQuery("SELECT TENANT_ID FROM T_CURRENT_TENANT WHERE TENANT_ID = '" + tenantId + "'")) {
            assertTrue(rs.next());
            assertEquals(rs.getString(1), tenantId);
            assertFalse(rs.next());

        } finally {
            MyTenantContext.clearTenantId();
        }
    }

    private static LightConfig createH2LightConfig() {
        val config = TestElf.newLightConfig();
        config.setMinIdle(1);
        config.setMaxPoolSize(2);
        config.setConnectionTestQuery("SELECT 1");
        config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
        config.addDataSourceProperty("url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
        return config;
    }

}
