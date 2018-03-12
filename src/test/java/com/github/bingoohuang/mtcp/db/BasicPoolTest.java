/*
 * Copyright (C) 2016 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.bingoohuang.mtcp.db;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.pool.LightPool;
import com.github.bingoohuang.mtcp.pool.TestElf;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author brettw
 */
public class BasicPoolTest {
   @Before
   public void setup() throws SQLException {
      LightConfig config = TestElf.newLightConfig();
      config.setMinimumIdle(1);
      config.setMaximumPoolSize(2);
      config.setConnectionTestQuery("SELECT 1");
      config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
      config.addDataSourceProperty("url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");

      try (LightDataSource ds = new LightDataSource(config);
           Connection conn = ds.getConnection();
           Statement stmt = conn.createStatement()) {
         stmt.executeUpdate("DROP TABLE IF EXISTS basic_pool_test");
         stmt.executeUpdate("CREATE TABLE basic_pool_test ("
            + "id INTEGER NOT NULL IDENTITY PRIMARY KEY, "
            + "timestamp TIMESTAMP, "
            + "string VARCHAR(128), "
            + "string_from_number NUMERIC "
            + ")");
      }
   }

   @Test
   public void testIdleTimeout() throws InterruptedException, SQLException {
      LightConfig config = TestElf.newLightConfig();
      config.setMinimumIdle(5);
      config.setMaximumPoolSize(10);
      config.setConnectionTestQuery("SELECT 1");
      config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
      config.addDataSourceProperty("url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");

      System.setProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs", "1000");

      try (LightDataSource ds = new LightDataSource(config)) {
         System.clearProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs");

         SECONDS.sleep(1);

         LightPool pool = TestElf.getPool(ds);

         TestElf.getUnsealedConfig(ds).setIdleTimeout(3000);

         assertEquals("Total connections not as expected", 5, pool.getTotalConnections());
         assertEquals("Idle connections not as expected", 5, pool.getIdleConnections());

         try (Connection connection = ds.getConnection()) {
            Assert.assertNotNull(connection);

            MILLISECONDS.sleep(1500);

            assertEquals("Second total connections not as expected", 6, pool.getTotalConnections());
            assertEquals("Second idle connections not as expected", 5, pool.getIdleConnections());
         }

         assertEquals("Idle connections not as expected", 6, pool.getIdleConnections());

         SECONDS.sleep(2);

         assertEquals("Third total connections not as expected", 5, pool.getTotalConnections());
         assertEquals("Third idle connections not as expected", 5, pool.getIdleConnections());
      }
   }

   @Test
   public void testIdleTimeout2() throws InterruptedException, SQLException {
      LightConfig config = TestElf.newLightConfig();
      config.setMaximumPoolSize(50);
      config.setConnectionTestQuery("SELECT 1");
      config.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
      config.addDataSourceProperty("url", "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");

      System.setProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs", "1000");

      try (LightDataSource ds = new LightDataSource(config)) {
         System.clearProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs");

         SECONDS.sleep(1);

         LightPool pool = TestElf.getPool(ds);

         TestElf.getUnsealedConfig(ds).setIdleTimeout(3000);

         assertEquals("Total connections not as expected", 50, pool.getTotalConnections());
         assertEquals("Idle connections not as expected", 50, pool.getIdleConnections());

         try (Connection connection = ds.getConnection()) {
            assertNotNull(connection);

            MILLISECONDS.sleep(1500);

            assertEquals("Second total connections not as expected", 50, pool.getTotalConnections());
            assertEquals("Second idle connections not as expected", 49, pool.getIdleConnections());
         }

         assertEquals("Idle connections not as expected", 50, pool.getIdleConnections());

         SECONDS.sleep(3);

         assertEquals("Third total connections not as expected", 50, pool.getTotalConnections());
         assertEquals("Third idle connections not as expected", 50, pool.getIdleConnections());
      }
   }
}
