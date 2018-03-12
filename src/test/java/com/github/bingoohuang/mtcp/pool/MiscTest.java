/*
 * Copyright (C) 2013 Brett Wooldridge
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

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import org.apache.logging.log4j.Level;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

/**
 * @author Brett Wooldridge
 */
public class MiscTest {
    @Test
    public void testLogWriter() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(4);
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
        TestElf.setConfigUnitTest(true);

        try (LightDataSource ds = new LightDataSource(config)) {
            PrintWriter writer = new PrintWriter(System.out);
            ds.setLogWriter(writer);
            assertSame(writer, ds.getLogWriter());
            assertEquals("testLogWriter", config.getPoolName());
        } finally {
            TestElf.setConfigUnitTest(false);
        }
    }

    @Test
    public void testInvalidIsolation() {
        try {
            UtilityElf.getTransactionIsolation("INVALID");
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testCreateInstance() {
        try {
            UtilityElf.createInstance("invalid", null);
            fail();
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof ClassNotFoundException);
        }
    }

    @Test
    public void testLeakDetection() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (PrintStream ps = new PrintStream(baos, true)) {
            TestElf.setSlf4jTargetStream(Class.forName("com.github.bingoohuang.mtcp.pool.ProxyLeakTask"), ps);
            TestElf.setConfigUnitTest(true);

            LightConfig config = TestElf.newLightConfig();
            config.setMinimumIdle(0);
            config.setMaximumPoolSize(4);
            config.setThreadFactory(Executors.defaultThreadFactory());
            config.setMetricRegistry(null);
            config.setLeakDetectionThreshold(TimeUnit.SECONDS.toMillis(1));
            config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

            try (LightDataSource ds = new LightDataSource(config)) {
                TestElf.setSlf4jLogLevel(LightPool.class, Level.DEBUG);
                TestElf.getPool(ds).logPoolState();

                try (Connection connection = ds.getConnection()) {
                    UtilityElf.quietlySleep(SECONDS.toMillis(4));
                    connection.close();
                    UtilityElf.quietlySleep(SECONDS.toMillis(1));
                    ps.close();
                    String s = new String(baos.toByteArray());
                    assertNotNull("Exception string was null", s);
                    assertTrue("Expected exception to contain 'Connection leak detection' but contains *" + s + "*", s.contains("Connection leak detection"));
                }
            } finally {
                TestElf.setConfigUnitTest(false);
                TestElf.setSlf4jLogLevel(LightPool.class, Level.INFO);
            }
        }
    }
}
