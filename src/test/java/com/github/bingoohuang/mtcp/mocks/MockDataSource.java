package com.github.bingoohuang.mtcp.mocks;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.logging.Logger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * @author Brett Wooldridge
 */
public class MockDataSource implements DataSource {
    @Override
    public Connection getConnection() throws SQLException {
        return createMockConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {
    }

    @Override
    public void setLoginTimeout(int seconds) {
    }

    @Override
    public int getLoginTimeout() {
        return 0;
    }

    public Logger getParentLogger() {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    public static Connection createMockConnection() throws SQLException {
        // Setup mock connection
        final Connection mockConnection = mock(Connection.class);

        // Autocommit is always true by default
        when(mockConnection.getAutoCommit()).thenReturn(true);

        // Handle Connection.createStatement()
        Statement statement = mock(Statement.class);
        when(mockConnection.createStatement()).thenReturn(statement);
        when(mockConnection.createStatement(anyInt(), anyInt())).thenReturn(statement);
        when(mockConnection.createStatement(anyInt(), anyInt(), anyInt())).thenReturn(statement);
        when(mockConnection.isValid(anyInt())).thenReturn(true);

        // Handle Connection.prepareStatement()
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
        when(mockConnection.prepareStatement(anyString(), anyInt())).thenReturn(mockPreparedStatement);
        when(mockConnection.prepareStatement(anyString(), any(int[].class))).thenReturn(mockPreparedStatement);
        when(mockConnection.prepareStatement(anyString(), any(String[].class))).thenReturn(mockPreparedStatement);
        when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt())).thenReturn(mockPreparedStatement);
        when(mockConnection.prepareStatement(anyString(), anyInt(), anyInt(), anyInt())).thenReturn(mockPreparedStatement);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) {
                return null;
            }
        }).doNothing().when(mockPreparedStatement).setInt(anyInt(), anyInt());

        ResultSet mockResultSet = mock(ResultSet.class);
        when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet);
        when(mockResultSet.getString(anyInt())).thenReturn("aString");
        when(mockResultSet.next()).thenReturn(true);

        // Handle Connection.prepareCall()
        CallableStatement mockCallableStatement = mock(CallableStatement.class);
        when(mockConnection.prepareCall(anyString())).thenReturn(mockCallableStatement);
        when(mockConnection.prepareCall(anyString(), anyInt(), anyInt())).thenReturn(mockCallableStatement);
        when(mockConnection.prepareCall(anyString(), anyInt(), anyInt(), anyInt())).thenReturn(mockCallableStatement);

        // Handle Connection.close()
//        doAnswer(new Answer<Void>() {
//            public Void answer(InvocationOnMock invocation) throws Throwable {
//                return null;
//            }
//        }).doThrow(new SQLException("Connection is already closed")).when(mockConnection).close();

        // Handle Connection.commit()
//        doAnswer(new Answer<Void>() {
//            public Void answer(InvocationOnMock invocation) throws Throwable {
//                return null;
//            }
//        }).doThrow(new SQLException("Transaction already commited")).when(mockConnection).commit();

        // Handle Connection.rollback()
//        doAnswer(new Answer<Void>() {
//            public Void answer(InvocationOnMock invocation) throws Throwable {
//                return null;
//            }
//        }).doThrow(new SQLException("Transaction already rolledback")).when(mockConnection).rollback();

        return mockConnection;
    }
}
