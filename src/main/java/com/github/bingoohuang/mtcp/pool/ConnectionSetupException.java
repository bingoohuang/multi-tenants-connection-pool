package com.github.bingoohuang.mtcp.pool;

public class ConnectionSetupException extends Exception {
    private static final long serialVersionUID = 929872118275916521L;

    ConnectionSetupException(Throwable t) {
        super(t);
    }
}
