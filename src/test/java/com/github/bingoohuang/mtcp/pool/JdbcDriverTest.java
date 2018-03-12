/*
 * Copyright (C) 2014 Brett Wooldridge
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
package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.util.DriverDataSource;
import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JdbcDriverTest {
   private LightDataSource ds;

   @After
   public void teardown() {
      if (ds != null) {
         ds.close();
      }
   }

   @Test
   public void driverTest1() throws SQLException {
      LightConfig config = TestElf.newLightConfig();
      config.setMinimumIdle(1);
      config.setMaximumPoolSize(1);
      config.setConnectionTestQuery("VALUES 1");
      config.setDriverClassName("com.github.bingoohuang.mtcp.mocks.StubDriver");
      config.setJdbcUrl("jdbc:stub");
      config.addDataSourceProperty("user", "bart");
      config.addDataSourceProperty("password", "simpson");

      ds = new LightDataSource(config);

      assertTrue(ds.isWrapperFor(DriverDataSource.class));

      DriverDataSource unwrap = ds.unwrap(DriverDataSource.class);
      assertNotNull(unwrap);

      try (Connection connection = ds.getConnection()) {
         // test that getConnection() succeeds
      }
   }

   @Test
   public void driverTest2() throws SQLException {
      LightConfig config = TestElf.newLightConfig();

      config.setMinimumIdle(1);
      config.setMaximumPoolSize(1);
      config.setConnectionTestQuery("VALUES 1");
      config.setDriverClassName("com.github.bingoohuang.mtcp.mocks.StubDriver");
      config.setJdbcUrl("jdbc:invalid");

      try {
         ds = new LightDataSource(config);
      } catch (RuntimeException e) {
         assertTrue(e.getMessage().contains("claims to not accept"));
      }
   }
}
