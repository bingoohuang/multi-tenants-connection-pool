package com.github.bingoohuang.mtcp.pool;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public interface LightPoolConstants {
    int POOL_NORMAL = 0;
    int POOL_SHUTDOWN = 2;

    long ALIVE_BYPASS_WINDOW_MS = Long.getLong(
            "com.github.bingoohuang.mtcp.aliveBypassWindowMs", MILLISECONDS.toMillis(500));
    long HOUSEKEEPING_PERIOD_MS = Long.getLong(
            "com.github.bingoohuang.mtcp.housekeeping.periodMs", SECONDS.toMillis(30));

    String EVICTED_CONNECTION_MESSAGE = "(connection was evicted)";
    String DEAD_CONNECTION_MESSAGE = "(connection is dead)";
}
