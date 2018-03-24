package com.github.bingoohuang.mtcp.mocks;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author Brett Wooldridge
 */
public class StubDriver implements Driver {
    private static final Driver driver;

    static {
        driver = new StubDriver();
        try {
            DriverManager.registerDriver(driver);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection connect(String url, Properties info) {
        return new StubConnection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acceptsURL(String url) {
        return "jdbc:stub".equals(url);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMajorVersion() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMinorVersion() {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public Logger getParentLogger() {
        return null;
    }
}
