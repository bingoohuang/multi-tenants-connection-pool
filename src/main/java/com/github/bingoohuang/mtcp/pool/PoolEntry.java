package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.util.BagEntry;
import com.github.bingoohuang.mtcp.util.ClockSource;
import com.github.bingoohuang.mtcp.util.FastList;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ScheduledFuture;


/**
 * Entry used in the ConcurrentBag to track Connection instances.
 *
 * @author Brett Wooldridge
 */
@Slf4j final class PoolEntry extends BagEntry {
    Connection connection;
    long lastAccessed;
    long lastBorrowed;

    @SuppressWarnings("FieldCanBeLocal")
    private volatile boolean evict;

    private volatile ScheduledFuture<?> endOfLife;

    private final FastList<Statement> openStatements;
    private final PoolBase pool;

    private final boolean isReadOnly;
    private final boolean isAutoCommit;

    @Getter private final int connectionSeq;

    PoolEntry(Connection connection, PoolBase pool, boolean isReadOnly, boolean isAutoCommit,  int connectionSeq) {
        this.connection = connection;
        this.pool = pool;
        this.isReadOnly = isReadOnly;
        this.isAutoCommit = isAutoCommit;
        this.lastAccessed = ClockSource.currentTime();
        this.openStatements = new FastList<>(Statement.class, 16);
        this.connectionSeq  = connectionSeq;
    }

    /**
     * Release this entry back to the pool.
     *
     * @param lastAccessed last access time-stamp
     */
    void recycle(long lastAccessed) {
        if (connection != null) {
            this.lastAccessed = lastAccessed;
            pool.recycle(this);
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

    LightProxyConnection createProxyConnection(ProxyLeakTask leakTask, long now) {
        return ProxyFactory.getProxyConnection(this, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
    }

    void resetConnectionState(final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException {
        pool.resetConnectionState(connection, proxyConnection, dirtyBits);
    }

    String getPoolName() {
        return pool.toString();
    }

    boolean isMarkedEvicted() {
        return evict;
    }

    void markEvicted() {
        this.evict = true;
    }

    void evict(final String closureReason) {
        pool.closeConnection(this, closureReason);
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
    //                      BagEntry methods
    // ***********************************************************************

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
}
