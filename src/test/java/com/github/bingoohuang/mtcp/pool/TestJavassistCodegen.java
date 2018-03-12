package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.mocks.StubConnection;
import com.github.bingoohuang.mtcp.util.JavassistProxyFactory;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Statement;
import java.util.stream.Stream;

public class TestJavassistCodegen {
    @Test
    public void testCodegen() throws Exception {
        String tmp = System.getProperty("java.io.tmpdir");
        JavassistProxyFactory.main(tmp + (tmp.endsWith("/") ? "" : "/"));

        Path base = Paths.get(tmp, "target/classes/com/github/bingoohuang/mtcp/pool".split("/"));
        Assert.assertTrue("", Files.isRegularFile(base.resolve("LightProxyConnection.class")));
        Assert.assertTrue("", Files.isRegularFile(base.resolve("LightProxyStatement.class")));
        Assert.assertTrue("", Files.isRegularFile(base.resolve("LightProxyCallableStatement.class")));
        Assert.assertTrue("", Files.isRegularFile(base.resolve("LightProxyPreparedStatement.class")));
        Assert.assertTrue("", Files.isRegularFile(base.resolve("LightProxyResultSet.class")));
        Assert.assertTrue("", Files.isRegularFile(base.resolve("ProxyFactory.class")));

        TestElf.FauxWebClassLoader fauxClassLoader = new TestElf.FauxWebClassLoader();
        Class<?> proxyFactoryClass = fauxClassLoader.loadClass("com.github.bingoohuang.mtcp.pool.ProxyFactory");

        Connection connection = new StubConnection();

        Class<?> fastListClass = fauxClassLoader.loadClass("com.github.bingoohuang.mtcp.util.FastList");
        Object fastList = fastListClass.getConstructor(Class.class).newInstance(Statement.class);

        Object proxyConnection = getMethod(proxyFactoryClass, "getProxyConnection")
                .invoke(null,
                        null /*poolEntry*/,
                        connection,
                        fastList,
                        null /*leakTask*/,
                        0L /*now*/,
                        Boolean.FALSE /*isReadOnly*/,
                        Boolean.FALSE /*isAutoCommit*/);
        Assert.assertNotNull(proxyConnection);

        Object proxyStatement = getMethod(proxyConnection.getClass(), "createStatement", 0)
                .invoke(proxyConnection);
        Assert.assertNotNull(proxyStatement);
    }

    private Method getMethod(Class<?> clazz, String methodName, Integer... parameterCount) {
        return Stream.of(clazz.getDeclaredMethods())
                .filter(method -> method.getName().equals(methodName))
                .filter(method -> (parameterCount.length == 0 || parameterCount[0] == method.getParameterCount()))
                .peek(method -> method.setAccessible(true))
                .findFirst()
                .orElseThrow(RuntimeException::new);
    }
}
