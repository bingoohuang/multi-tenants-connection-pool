/*
 * Copyright (C) 2014 Brett Wooldridge
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
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * @author Brett Wooldridge
 */
public class TestValidation {
    @Test
    public void validateLoadProperties() {
        System.setProperty("lightcp.configurationFile", "/propfile1.properties");
        LightConfig config = TestElf.newLightConfig();
        System.clearProperty("lightcp.configurationFile");
        assertEquals(5, config.getMinimumIdle());
    }

    @Test
    public void validateMissingProperties() {
        try {
            LightConfig config = new LightConfig("missing");
            config.validate();
        } catch (IllegalArgumentException ise) {
            assertTrue(ise.getMessage().contains("property file"));
        }
    }

    @Test
    public void validateMissingDS() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.validate();
            fail();
        } catch (IllegalArgumentException ise) {
            assertTrue(ise.getMessage().contains("dataSource or dataSourceClassName or jdbcUrl is required."));
        }
    }

    @Test
    public void validateMissingUrl() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setDriverClassName("com.github.bingoohuang.mtcp.mocks.StubDriver");
            config.validate();
            fail();
        } catch (IllegalArgumentException ise) {
            assertTrue(ise.getMessage().contains("jdbcUrl is required with driverClassName"));
        }
    }

    @Test
    public void validateDriverAndUrl() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setDriverClassName("com.github.bingoohuang.mtcp.mocks.StubDriver");
            config.setJdbcUrl("jdbc:stub");
            config.validate();
        } catch (Throwable t) {
            fail(t.getMessage());
        }
    }

    @Test
    public void validateBadDriver() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setDriverClassName("invalid");
            config.validate();
            fail();
        } catch (RuntimeException ise) {
            assertTrue(ise.getMessage().startsWith("Failed to load driver class invalid "));
        }
    }

    @Test
    public void validateInvalidConnectionTimeout() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setConnectionTimeout(10L);
            fail();
        } catch (IllegalArgumentException ise) {
            assertTrue(ise.getMessage().contains("connectionTimeout cannot be less than 250ms"));
        }
    }

    @Test
    public void validateInvalidValidationTimeout() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setValidationTimeout(10L);
            fail();
        } catch (IllegalArgumentException ise) {
            assertTrue(ise.getMessage().contains("validationTimeout cannot be less than 250ms"));
        }
    }

    @Test
    public void validateInvalidIdleTimeout() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setIdleTimeout(-1L);
            fail("negative idle timeout accepted");
        } catch (IllegalArgumentException ise) {
            assertTrue(ise.getMessage().contains("idleTimeout cannot be negative"));
        }
    }

    @Test
    public void validateIdleTimeoutTooSmall() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true);
        TestElf.setSlf4jTargetStream(LightConfig.class, ps);

        LightConfig config = TestElf.newLightConfig();
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
        config.setIdleTimeout(TimeUnit.SECONDS.toMillis(5));
        config.validate();
        assertTrue(new String(baos.toByteArray()).contains("less than 10000ms"));
    }

    @Test
    public void validateIdleTimeoutExceedsLifetime() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true);
        TestElf.setSlf4jTargetStream(LightConfig.class, ps);

        LightConfig config = TestElf.newLightConfig();
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
        config.setMaxLifetime(TimeUnit.MINUTES.toMillis(2));
        config.setIdleTimeout(TimeUnit.MINUTES.toMillis(3));
        config.validate();

        String s = new String(baos.toByteArray());
        assertTrue("idleTimeout is close to or more than maxLifetime, disabling it." + s + "*", s.contains("is close to or more than maxLifetime"));
    }

    @Test
    public void validateInvalidMinIdle() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setMinimumIdle(-1);
            fail();
        } catch (IllegalArgumentException ise) {
            assertTrue(ise.getMessage().contains("minimumIdle cannot be negative"));
        }
    }

    @Test
    public void validateInvalidMaxPoolSize() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setMaximumPoolSize(0);
            fail();
        } catch (IllegalArgumentException ise) {
            assertTrue(ise.getMessage().contains("maxPoolSize cannot be less than 1"));
        }
    }

    @Test
    public void validateInvalidLifetime() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setConnectionTimeout(Integer.MAX_VALUE);
            config.setIdleTimeout(1000L);
            config.setMaxLifetime(-1L);
            config.setLeakDetectionThreshold(1000L);
            config.validate();
            fail();
        } catch (IllegalArgumentException ise) {
            // pass
        }
    }

    @Test
    public void validateInvalidLeakDetection() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setLeakDetectionThreshold(1000L);
            config.validate();
            fail();
        } catch (IllegalArgumentException ise) {
            // pass
        }
    }

    @Test
    public void validateZeroConnectionTimeout() {
        try {
            LightConfig config = TestElf.newLightConfig();
            config.setConnectionTimeout(0);
            config.validate();
            assertEquals(Integer.MAX_VALUE, config.getConnectionTimeout());
        } catch (IllegalArgumentException ise) {
            // pass
        }
    }
}
