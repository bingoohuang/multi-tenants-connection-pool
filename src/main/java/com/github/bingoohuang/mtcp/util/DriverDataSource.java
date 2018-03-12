/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
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
package com.github.bingoohuang.mtcp.util;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Properties;

@Slf4j
public final class DriverDataSource implements DataSource {
   private final String jdbcUrl;
   private final Properties driverProperties;
   private Driver driver;

   public DriverDataSource(String jdbcUrl, String driverClassName, Properties properties, String username, String password) {
      this.jdbcUrl = jdbcUrl;
      this.driverProperties = new Properties();

      for (val entry : properties.entrySet()) {
         driverProperties.setProperty(entry.getKey().toString(), entry.getValue().toString());
      }

      if (username != null) {
         driverProperties.put("user", driverProperties.getProperty("user", username));
      }
      if (password != null) {
         driverProperties.put("password", driverProperties.getProperty("password", password));
      }

      if (driverClassName != null) {
         val drivers = DriverManager.getDrivers();
         while (drivers.hasMoreElements()) {
            val d = drivers.nextElement();
            if (d.getClass().getName().equals(driverClassName)) {
               driver = d;
               break;
            }
         }

         if (driver == null) {
            log.warn("Registered driver with driverClassName={} was not found, trying direct instantiation.", driverClassName);
            Class<?> driverClass = null;
            val threadContextClassLoader = Thread.currentThread().getContextClassLoader();
            try {
               if (threadContextClassLoader != null) {
                  try {
                     driverClass = threadContextClassLoader.loadClass(driverClassName);
                     log.debug("Driver class {} found in Thread context class loader {}", driverClassName, threadContextClassLoader);
                  } catch (ClassNotFoundException e) {
                     log.debug("Driver class {} not found in Thread context class loader {}, trying classloader {}",
                        driverClassName, threadContextClassLoader, this.getClass().getClassLoader());
                  }
               }

               if (driverClass == null) {
                  driverClass = this.getClass().getClassLoader().loadClass(driverClassName);
                  log.debug("Driver class {} found in the LightConfig class classloader {}", driverClassName, this.getClass().getClassLoader());
               }
            } catch (ClassNotFoundException e) {
               log.debug("Failed to load driver class {} from LightConfig class classloader {}", driverClassName, this.getClass().getClassLoader());
            }

            if (driverClass != null) {
               try {
                  driver = (Driver) driverClass.newInstance();
               } catch (Exception e) {
                  log.warn("Failed to create instance of driver class {}, trying jdbcUrl resolution", driverClassName, e);
               }
            }
         }
      }

      try {
         if (driver == null) {
            driver = DriverManager.getDriver(jdbcUrl);
         } else if (!driver.acceptsURL(jdbcUrl)) {
            throw new RuntimeException("Driver " + driverClassName + " claims to not accept jdbcUrl, " + jdbcUrl);
         }
      } catch (SQLException e) {
         throw new RuntimeException("Failed to get driver instance for jdbcUrl=" + jdbcUrl, e);
      }
   }

   @Override
   public Connection getConnection() throws SQLException {
      return driver.connect(jdbcUrl, driverProperties);
   }

   @Override
   public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
   }

   @Override
   public PrintWriter getLogWriter() throws SQLException {
      throw new SQLFeatureNotSupportedException();
   }

   @Override
   public void setLogWriter(PrintWriter logWriter) throws SQLException {
      throw new SQLFeatureNotSupportedException();
   }

   @Override
   public void setLoginTimeout(int seconds) {
      DriverManager.setLoginTimeout(seconds);
   }

   @Override
   public int getLoginTimeout() {
      return DriverManager.getLoginTimeout();
   }

   @Override
   public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return driver.getParentLogger();
   }

   @Override
   public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new SQLFeatureNotSupportedException();
   }

   @Override
   public boolean isWrapperFor(Class<?> iface) {
      return false;
   }
}
