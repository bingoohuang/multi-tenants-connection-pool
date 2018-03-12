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

import com.github.bingoohuang.mtcp.util.ClockSource;
import com.github.bingoohuang.mtcp.util.ConcurrentBag;
import com.github.bingoohuang.mtcp.util.FastList;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;


/**
 * Entry used in the ConcurrentBag to track Connection instances.
 *
 * @author Brett Wooldridge
 */
@Slf4j final class PoolEntry implements ConcurrentBag.IConcurrentBagEntry {
    private static final AtomicIntegerFieldUpdater<PoolEntry> stateUpdater
            = AtomicIntegerFieldUpdater.newUpdater(PoolEntry.class, "state");

    Connection connection;
    long lastAccessed;
    long lastBorrowed;

    @SuppressWarnings("FieldCanBeLocal")
    private volatile int state = 0;
    private volatile boolean evict;

    private volatile ScheduledFuture<?> endOfLife;

    private final FastList<Statement> openStatements;
    private final PoolBase lightPool;

    private final boolean isReadOnly;
    private final boolean isAutoCommit;

    PoolEntry(Connection connection, final PoolBase pool, final boolean isReadOnly, final boolean isAutoCommit) {
        this.connection = connection;
        this.lightPool = pool;
        this.isReadOnly = isReadOnly;
        this.isAutoCommit = isAutoCommit;
        this.lastAccessed = ClockSource.currentTime();
        this.openStatements = new FastList<>(Statement.class, 16);
    }

    /**
     * Release this entry back to the pool.
     *
     * @param lastAccessed last access time-stamp
     */
    void recycle(final long lastAccessed) {
        if (connection != null) {
            this.lastAccessed = lastAccessed;
            lightPool.recycle(this);
        }
    }

    /**
     * Set the end of life {@link ScheduledFuture}.
     *
     * @param endOfLife this PoolEntry/Connection's end of life {@link ScheduledFuture}
     */
    void setFutureEol(final ScheduledFuture<?> endOfLife) {
        this.endOfLife = endOfLife;
    }

    Connection createProxyConnection(final ProxyLeakTask leakTask, final long now) {
        return ProxyFactory.getProxyConnection(this, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
    }

    void resetConnectionState(final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException {
        lightPool.resetConnectionState(connection, proxyConnection, dirtyBits);
    }

    String getPoolName() {
        return lightPool.toString();
    }

    boolean isMarkedEvicted() {
        return evict;
    }

    void markEvicted() {
        this.evict = true;
    }

    void evict(final String closureReason) {
        lightPool.closeConnection(this, closureReason);
    }

    /**
     * Returns millis since lastBorrowed
     */
    long getMillisSinceBorrowed() {
        return ClockSource.elapsedMillis(lastBorrowed);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        val now = ClockSource.currentTime();
        return connection
                + ", accessed " + ClockSource.elapsedDisplayString(lastAccessed, now) + " ago, "
                + stateToString();
    }

    // ***********************************************************************
    //                      IConcurrentBagEntry methods
    // ***********************************************************************

    /**
     * {@inheritDoc}
     */
    @Override
    public int getState() {
        return stateUpdater.get(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean compareAndSet(int expect, int update) {
        return stateUpdater.compareAndSet(this, expect, update);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setState(int update) {
        stateUpdater.set(this, update);
    }

    Connection close() {
        val eol = endOfLife;
        if (eol != null && !eol.isDone() && !eol.cancel(false)) {
            log.warn("{} - maxLifeTime expiration task cancellation unexpectedly returned false for connection {}", getPoolName(), connection);
        }

        val con = connection;
        connection = null;
        endOfLife = null;
        return con;
    }

    private String stateToString() {
        switch (state) {
            case STATE_USING:
                return "USING";
            case STATE_FREE:
                return "FREE";
            case STATE_REMOVED:
                return "REMOVED";
            case STATE_RESERVED:
                return "RESERVED";
            default:
                return "Invalid";
        }
    }
}