/*
 * Copyright (C) 2013 Brett Wooldridge
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

package com.github.bingoohuang.mtcp;

import com.github.bingoohuang.mtcp.metrics.MetricsTrackerFactory;
import com.github.bingoohuang.mtcp.pool.LightPool;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The LightCP pooled DataSource.
 *
 * @author Brett Wooldridge
 */
@Slf4j
public class LightDataSource extends LightConfig implements DataSource, Closeable {
   private final AtomicBoolean isShutdown = new AtomicBoolean();

   private final LightPool fastPathPool;
   private volatile LightPool pool;

   /**
    * Default constructor.  Setters are used to configure the pool.  Using
    * this constructor vs. {@link #LightDataSource(LightConfig)} will
    * result in {@link #getConnection()} performance that is slightly lower
    * due to lazy initialization checks.
    * <p>
    * The first call to {@link #getConnection()} starts the pool.  Once the pool
    * is started, the configuration is "sealed" and no further configuration
    * changes are possible -- except via {@link LightConfigMXBean} methods.
    */
   public LightDataSource() {
      super();
      fastPathPool = null;
   }

   /**
    * Construct a LightDataSource with the specified configuration.  The
    * {@link LightConfig} is copied and the pool is started by invoking this
    * constructor.
    * <p>
    * The {@link LightConfig} can be modified without affecting the LightDataSource
    * and used to initialize another LightDataSource instance.
    *
    * @param configuration a LightConfig instance
    */
   public LightDataSource(LightConfig configuration) {
      configuration.validate();
      configuration.copyStateTo(this);

      val poolName = configuration.getPoolName();
      log.info("{} - Starting...", poolName);
      pool = fastPathPool = new LightPool(this);
      log.info("{} - Started.", poolName);

      this.seal();
   }

   // ***********************************************************************
   //                          DataSource methods
   // ***********************************************************************

   /**
    * {@inheritDoc}
    */
   @Override
   public Connection getConnection() throws SQLException {
      if (isClosed()) {
         throw new SQLException("LightDataSource " + this + " has been closed.");
      }

      if (fastPathPool != null) {
         return fastPathPool.getConnection();
      }

      // See http://en.wikipedia.org/wiki/Double-checked_locking#Usage_in_Java
      LightPool result = pool;
      if (result == null) {
         synchronized (this) {
            result = pool;
            if (result == null) {
               validate();
               result = startPool();
            }
         }
      }

      return result.getConnection();
   }

   private LightPool startPool() throws SQLException {
      LightPool result;
      val poolName = getPoolName();
      log.info("{} - Starting...", poolName);
      try {
         pool = result = new LightPool(this);
         this.seal();
      } catch (LightPool.PoolInitializationException pie) {
         if (pie.getCause() instanceof SQLException) {
            throw (SQLException) pie.getCause();
         } else {
            throw pie;
         }
      }
      log.info("{} - Started.", poolName);
      return result;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public Connection getConnection(String username, String password) throws SQLException {
      throw new SQLFeatureNotSupportedException();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public PrintWriter getLogWriter() throws SQLException {
      LightPool p = pool;
      return (p != null ? p.getUnwrappedDataSource().getLogWriter() : null);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setLogWriter(PrintWriter out) throws SQLException {
      LightPool p = pool;
      if (p != null) {
         p.getUnwrappedDataSource().setLogWriter(out);
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setLoginTimeout(int seconds) throws SQLException {
      LightPool p = pool;
      if (p != null) {
         p.getUnwrappedDataSource().setLoginTimeout(seconds);
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public int getLoginTimeout() throws SQLException {
      LightPool p = pool;
      return (p != null ? p.getUnwrappedDataSource().getLoginTimeout() : 0);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   @SuppressWarnings("unchecked")
   public <T> T unwrap(Class<T> iface) throws SQLException {
      if (iface.isInstance(this)) {
         return (T) this;
      }

      LightPool p = pool;
      if (p != null) {
         val unwrappedDataSource = p.getUnwrappedDataSource();
         if (iface.isInstance(unwrappedDataSource)) {
            return (T) unwrappedDataSource;
         }

         if (unwrappedDataSource != null) {
            return unwrappedDataSource.unwrap(iface);
         }
      }

      throw new SQLException("Wrapped DataSource is not an instance of " + iface);
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public boolean isWrapperFor(Class<?> iface) throws SQLException {
      if (iface.isInstance(this)) {
         return true;
      }

      LightPool p = pool;
      if (p != null) {
         val unwrappedDataSource = p.getUnwrappedDataSource();
         if (iface.isInstance(unwrappedDataSource)) {
            return true;
         }

         if (unwrappedDataSource != null) {
            return unwrappedDataSource.isWrapperFor(iface);
         }
      }

      return false;
   }

   // ***********************************************************************
   //                        LightConfigMXBean methods
   // ***********************************************************************

   /**
    * {@inheritDoc}
    */
   @Override
   public void setMetricRegistry(Object metricRegistry) {
      boolean isAlreadySet = getMetricRegistry() != null;
      super.setMetricRegistry(metricRegistry);

      LightPool p = pool;
      if (p != null) {
         if (isAlreadySet) {
            throw new IllegalStateException("MetricRegistry can only be set one time");
         } else {
            p.setMetricRegistry(super.getMetricRegistry());
         }
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {
      boolean isAlreadySet = getMetricsTrackerFactory() != null;
      super.setMetricsTrackerFactory(metricsTrackerFactory);

      LightPool p = pool;
      if (p != null) {
         if (isAlreadySet) {
            throw new IllegalStateException("MetricsTrackerFactory can only be set one time");
         } else {
            p.setMetricsTrackerFactory(super.getMetricsTrackerFactory());
         }
      }
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void setHealthCheckRegistry(Object healthCheckRegistry) {
      boolean isAlreadySet = getHealthCheckRegistry() != null;
      super.setHealthCheckRegistry(healthCheckRegistry);

      LightPool p = pool;
      if (p != null) {
         if (isAlreadySet) {
            throw new IllegalStateException("HealthCheckRegistry can only be set one time");
         } else {
            p.setHealthCheckRegistry(super.getHealthCheckRegistry());
         }
      }
   }

   // ***********************************************************************
   //                        LightCP-specific methods
   // ***********************************************************************

   /**
    * Returns {@code true} if the pool as been started and is not suspended or shutdown.
    *
    * @return {@code true} if the pool as been started and is not suspended or shutdown.
    */
   public boolean isRunning() {
      return pool != null && pool.poolState != LightPool.POOL_SHUTDOWN;
   }

   /**
    * Get the {@code LightPoolMXBean} for this LightDataSource instance.  If this method is called on
    * a {@code LightDataSource} that has been constructed without a {@code LightConfig} instance,
    * and before an initial call to {@code #getConnection()}, the return value will be {@code null}.
    *
    * @return the {@code LightPoolMXBean} instance, or {@code null}.
    */
   public LightPoolMXBean getLightPoolMXBean() {
      return pool;
   }

   /**
    * Get the {@code LightConfigMXBean} for this LightDataSource instance.
    *
    * @return the {@code LightConfigMXBean} instance.
    */
   public LightConfigMXBean getLightConfigMXBean() {
      return this;
   }

   /**
    * Evict a connection from the pool.  If the connection has already been closed (returned to the pool)
    * this may result in a "soft" eviction; the connection will be evicted sometime in the future if it is
    * currently in use.  If the connection has not been closed, the eviction is immediate.
    *
    * @param connection the connection to evict from the pool
    */
   public void evictConnection(Connection connection) {
      LightPool p;
      if (!isClosed() && (p = pool) != null && connection.getClass().getName().startsWith("com.github.bingoohuang.mtcp")) {
         p.evictConnection(connection);
      }
   }

   /**
    * Suspend allocation of connections from the pool.  All callers to <code>getConnection()</code>
    * will block indefinitely until <code>resumePool()</code> is called.
    *
    * @deprecated Call the {@code LightPoolMXBean#suspendPool()} method on the {@code LightPoolMXBean}
    * obtained by {@code #getLightPoolMXBean()} or JMX lookup.
    */
   @Deprecated
   public void suspendPool() {
      LightPool p;
      if (!isClosed() && (p = pool) != null) {
         p.suspendPool();
      }
   }

   /**
    * Resume allocation of connections from the pool.
    *
    * @deprecated Call the {@code LightPoolMXBean#resumePool()} method on the {@code LightPoolMXBean}
    * obtained by {@code #getLightPoolMXBean()} or JMX lookup.
    */
   @Deprecated
   public void resumePool() {
      LightPool p;
      if (!isClosed() && (p = pool) != null) {
         p.resumePool();
      }
   }

   /**
    * Shutdown the DataSource and its associated pool.
    */
   @Override
   public void close() {
      if (isShutdown.getAndSet(true)) {
         return;
      }

      LightPool p = pool;
      if (p != null) {
         val poolName = getPoolName();
         try {
            log.info("{} - Shutting down...", poolName);
            p.shutdown();
            log.info("{} - Stopped.", poolName);
         } catch (InterruptedException e) {
            log.warn("{} - Interrupted during closing", poolName, e);
            Thread.currentThread().interrupt();
         }
      }
   }

   /**
    * Determine whether the LightDataSource has been closed.
    *
    * @return true if the LightDataSource has been closed, false otherwise
    */
   public boolean isClosed() {
      return isShutdown.get();
   }

   /**
    * Shutdown the DataSource and its associated pool.
    *
    * @deprecated This method has been deprecated, please use {@link #close()} instead
    */
   @Deprecated
   public void shutdown() {
      log.warn("The shutdown() method has been deprecated, please use the close() method instead");
      close();
   }

   /**
    * {@inheritDoc}
    */
   @Override
   public String toString() {
      return "LightDataSource (" + pool + ")";
   }
}
