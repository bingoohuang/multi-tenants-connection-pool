package com.github.bingoohuang.mtcp.pool;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executor;

/**
 * Special executor used only to work around a MySQL issue that has not been addressed.
 * MySQL issue: http://bugs.mysql.com/bug.php?id=75615
 */
@Slf4j
public class SameThreadExecutor implements Executor {
    public static final Executor INSTANCE = new SameThreadExecutor();

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Runnable command) {
        try {
            command.run();
        } catch (Throwable t) {
            log.debug("Failed to execute: {}", command, t);
            throw t;
        }
    }
}
