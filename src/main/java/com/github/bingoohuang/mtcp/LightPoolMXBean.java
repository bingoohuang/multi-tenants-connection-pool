package com.github.bingoohuang.mtcp;

/**
 * The javax.management MBean for a Light pool instance.
 *
 * @author Brett Wooldridge
 */
public interface LightPoolMXBean {
    int getIdleConnections();

    int getActiveConnections();

    int getTotalConnections();

    int getThreadsAwaitingConnection();

    void softEvictConnections();
}
