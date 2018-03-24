/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
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
import com.github.bingoohuang.mtcp.util.PropertyElf;
import org.junit.Test;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.*;

public class TestPropertySetter {
    @Test
    public void testProperty1() throws Exception {
        Properties propfile1 = new Properties();
        propfile1.load(TestPropertySetter.class.getResourceAsStream("/propfile1.properties"));
        LightConfig config = new LightConfig(propfile1);
        config.validate();

        assertEquals(5, config.getMinimumIdle());
        assertEquals("SELECT 1", config.getConnectionTestQuery());
    }

    @Test
    public void testProperty2() throws Exception {
        Properties propfile2 = new Properties();
        propfile2.load(TestPropertySetter.class.getResourceAsStream("/propfile2.properties"));
        LightConfig config = new LightConfig(propfile2);
        config.validate();

        Class<?> clazz = this.getClass().getClassLoader().loadClass(config.getDataSourceClassName());
        DataSource dataSource = (DataSource) clazz.newInstance();
        PropertyElf.setTargetFromProperties(dataSource, config.getDataSourceProperties());
    }

    @Test
    public void testObjectProperty() throws Exception {
        LightConfig config = TestElf.newLightConfig();
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
        PrintWriter writer = new PrintWriter(new ByteArrayOutputStream());
        config.addDataSourceProperty("logWriter", writer);

        Class<?> clazz = this.getClass().getClassLoader().loadClass(config.getDataSourceClassName());
        DataSource dataSource = (DataSource) clazz.newInstance();
        PropertyElf.setTargetFromProperties(dataSource, config.getDataSourceProperties());

        assertSame(PrintWriter.class, dataSource.getLogWriter().getClass());
    }

    @Test
    public void testPropertyUpperCase() throws Exception {
        Properties propfile3 = new Properties();
        propfile3.load(TestPropertySetter.class.getResourceAsStream("/propfile3.properties"));
        LightConfig config = new LightConfig(propfile3);
        config.validate();

        Class<?> clazz = this.getClass().getClassLoader().loadClass(config.getDataSourceClassName());
        DataSource dataSource = (DataSource) clazz.newInstance();
        PropertyElf.setTargetFromProperties(dataSource, config.getDataSourceProperties());
    }

    @Test
    public void testGetPropertyNames() {
        Set<String> propertyNames = PropertyElf.getPropertyNames(LightConfig.class);
        assertTrue(propertyNames.contains("dataSourceClassName"));
    }

    @Test
    public void testSetNonExistantPropertyName() {
        try {
            Properties props = new Properties();
            props.put("what", "happened");
            PropertyElf.setTargetFromProperties(new LightConfig(), props);
            fail();
        } catch (RuntimeException e) {
        }
    }
}
