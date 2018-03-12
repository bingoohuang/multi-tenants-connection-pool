package com.github.bingoohuang.mtcp.pool;

public class PoolInitializationException extends RuntimeException {
    private static final long serialVersionUID = 929872118275916520L;

    /**
     * Construct an exception, possibly wrapping the provided Throwable as the cause.
     *
     * @param t the Throwable to wrap
     */
    public PoolInitializationException(Throwable t) {
        super("Failed to initialize pool: " + t.getMessage(), t);
    }
}
