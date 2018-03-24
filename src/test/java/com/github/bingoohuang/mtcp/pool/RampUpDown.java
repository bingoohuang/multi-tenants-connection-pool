package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertSame;

public class RampUpDown {
    @Test
    public void rampUpDownTest() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(60);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        System.setProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs", "250");

        try (LightDataSource ds = new LightDataSource(config)) {

            ds.setIdleTimeout(1000);
            LightPool pool = TestElf.getPool(ds);

            // wait two housekeeping periods so we don't fail if this part of test runs too quickly
            UtilityElf.quietlySleep(500);

            Assert.assertSame("Total connections not as expected", 5, pool.getTotalConnections());

            Connection[] connections = new Connection[ds.getMaximumPoolSize()];
            for (int i = 0; i < connections.length; i++) {
                connections[i] = ds.getConnection();
            }

            assertSame("Total connections not as expected", 60, pool.getTotalConnections());

            for (Connection connection : connections) {
                connection.close();
            }

            UtilityElf.quietlySleep(500);

            assertSame("Total connections not as expected", 5, pool.getTotalConnections());
        }
    }
}
