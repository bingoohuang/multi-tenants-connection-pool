package com.github.bingoohuang.mtcp.util;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

@Slf4j
public class DriverElf {
    public static Driver createDriver(String jdbcUrl, String driverClassName) {
        Driver driver = null;
        if (driverClassName != null) {
            driver = findDriver(driverClassName);

            if (driver == null) {
                log.warn("Registered driver with driverClassName={} was not found, trying direct instantiation.", driverClassName);
                Class<?> driverClass = loadDriverClass(driverClassName, false);
                if (driverClass != null) {
                    try {
                        driver = (Driver) driverClass.newInstance();
                    } catch (Exception e) {
                        log.warn("Failed to create instance of driver class {}, trying jdbcUrl resolution", driverClassName, e);
                    }
                }
            }
        }

        return checkJdbcUrlAccepts(jdbcUrl, driverClassName, driver);
    }

    private static Driver findDriver(String driverClassName) {
        val drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            val d = drivers.nextElement();
            if (d.getClass().getName().equals(driverClassName)) {
                return d;
            }
        }
        return null;
    }

    public static Class<?> loadDriverClass(String driverClassName, boolean logError) {
        Class<?> driverClass = null;
        val classLoader = Thread.currentThread().getContextClassLoader();
        try {
            if (classLoader != null) {
                try {
                    driverClass = classLoader.loadClass(driverClassName);
                    log.debug("Driver class {} found in Thread context class loader {}", driverClassName, classLoader);
                } catch (ClassNotFoundException e) {
                    log.debug("Driver class {} not found in Thread context class loader {}, trying classloader {}",
                            driverClassName, classLoader, DriverElf.class.getClassLoader());
                }
            }

            if (driverClass == null) {
                driverClass = DriverElf.class.getClassLoader().loadClass(driverClassName);
                log.debug("Driver class {} found in the LightConfig class classloader {}",
                        driverClassName, DriverElf.class.getClassLoader());
            }
        } catch (ClassNotFoundException e) {
            String format = "Failed to load driver class {} from LightConfig class classloader {}";
            if (logError) {
                log.error(format, driverClassName, DriverElf.class.getClassLoader());
            } else {
                log.debug(format, driverClassName, DriverElf.class.getClassLoader());
            }
        }
        return driverClass;
    }

    private static Driver checkJdbcUrlAccepts(String jdbcUrl, String driverClassName, Driver driver) {
        try {
            if (driver == null) {
                return DriverManager.getDriver(jdbcUrl);
            }

            if (!driver.acceptsURL(jdbcUrl)) {
                throw new RuntimeException("Driver " + driverClassName + " claims to not accept jdbcUrl, " + jdbcUrl);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get driver instance for jdbcUrl=" + jdbcUrl, e);
        }

        return driver;
    }
}
