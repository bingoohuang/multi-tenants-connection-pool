package com.github.bingoohuang.mtcp.datasource;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.github.bingoohuang.mtcp.pool.TestElf.newLightConfig;
import static org.junit.Assert.fail;

public class TestSealedConfig {
   @Test(expected = IllegalStateException.class)
   public void testSealed1() throws SQLException {
      LightConfig config = newLightConfig();
      config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

      try (LightDataSource ds = new LightDataSource(config)) {
         ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
         fail("Exception should have been thrown");
      }
   }

   @Test(expected = IllegalStateException.class)
   public void testSealed2() throws SQLException {
      LightDataSource ds = new LightDataSource();
      ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

      try (LightDataSource closeable = ds) {
         try (Connection connection = ds.getConnection()) {
            ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
            fail("Exception should have been thrown");
         }
      }
   }

   @Test(expected = IllegalStateException.class)
   public void testSealed3() throws SQLException {
      LightDataSource ds = new LightDataSource();
      ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

      try (LightDataSource closeable = ds) {
         try (Connection connection = ds.getConnection()) {
            ds.setAutoCommit(false);
            fail("Exception should have been thrown");
         }
      }
   }

   @Test
   public void testSealedAccessibleMethods() throws SQLException {
      LightConfig config = newLightConfig();
      config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

      try (LightDataSource ds = new LightDataSource(config)) {
         ds.setConnectionTimeout(5000);
         ds.setValidationTimeout(5000);
         ds.setIdleTimeout(30000);
         ds.setLeakDetectionThreshold(60000);
         ds.setMaxLifetime(1800000);
         ds.setMinimumIdle(5);
         ds.setMaximumPoolSize(8);
         ds.setPassword("password");
         ds.setUsername("username");
      }
   }
}
