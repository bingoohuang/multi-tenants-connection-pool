package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.LightJNDIFactory;
import com.github.bingoohuang.mtcp.mocks.StubDataSource;
import org.junit.Assert;
import org.junit.Test;
import org.osjava.sj.jndi.AbstractContext;

import javax.naming.*;
import java.sql.Connection;

import static org.junit.Assert.*;

public class TestJNDI {
    @Test
    public void testJndiLookup1() throws Exception {
        LightJNDIFactory jndi = new LightJNDIFactory();
        Reference ref = new Reference("javax.sql.DataSource");
        ref.add(new BogusRef("driverClassName", "com.github.bingoohuang.mtcp.mocks.StubDriver"));
        ref.add(new BogusRef("jdbcUrl", "jdbc:stub"));
        ref.add(new BogusRef("username", "foo"));
        ref.add(new BogusRef("password", "foo"));
        ref.add(new BogusRef("minimumIdle", "0"));
        ref.add(new BogusRef("maxLifetime", "30000"));
        ref.add(new BogusRef("maximumPoolSize", "10"));
        ref.add(new BogusRef("dataSource.loginTimeout", "10"));
        Context nameCtx = new BogusContext();

        try (LightDataSource ds = (LightDataSource) jndi.getObjectInstance(ref, null, nameCtx, null)) {
            assertNotNull(ds);
            Assert.assertEquals("foo", TestElf.getUnsealedConfig(ds).getUsername());
        }
    }

    @Test
    public void testJndiLookup2() throws Exception {
        LightJNDIFactory jndi = new LightJNDIFactory();
        Reference ref = new Reference("javax.sql.DataSource");
        ref.add(new BogusRef("dataSourceJNDI", "java:comp/env/LightDS"));
        ref.add(new BogusRef("driverClassName", "com.github.bingoohuang.mtcp.mocks.StubDriver"));
        ref.add(new BogusRef("jdbcUrl", "jdbc:stub"));
        ref.add(new BogusRef("username", "foo"));
        ref.add(new BogusRef("password", "foo"));
        ref.add(new BogusRef("minimumIdle", "0"));
        ref.add(new BogusRef("maxLifetime", "30000"));
        ref.add(new BogusRef("maximumPoolSize", "10"));
        ref.add(new BogusRef("dataSource.loginTimeout", "10"));
        Context nameCtx = new BogusContext2();

        try (LightDataSource ds = (LightDataSource) jndi.getObjectInstance(ref, null, nameCtx, null)) {
            assertNotNull(ds);
            Assert.assertEquals("foo", TestElf.getUnsealedConfig(ds).getUsername());
        }
    }

    @Test
    public void testJndiLookup3() throws Exception {
        LightJNDIFactory jndi = new LightJNDIFactory();

        Reference ref = new Reference("javax.sql.DataSource");
        ref.add(new BogusRef("dataSourceJNDI", "java:comp/env/LightDS"));
        try {
            jndi.getObjectInstance(ref, null, null, null);
            fail();
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("JNDI context does not found"));
        }
    }

    @Test
    public void testJndiLookup4() throws Exception {
        System.setProperty(Context.INITIAL_CONTEXT_FACTORY, "org.osjava.sj.memory.MemoryContextFactory");
        System.setProperty("org.osjava.sj.jndi.shared", "true");
        InitialContext ic = new InitialContext();

        StubDataSource ds = new StubDataSource();

        Context subcontext = ic.createSubcontext("java:/comp/env/jdbc");
        subcontext.bind("java:/comp/env/jdbc/myDS", ds);

        LightConfig config = TestElf.newLightConfig();
        config.setDataSourceJNDI("java:/comp/env/jdbc/myDS");

        try (LightDataSource hds = new LightDataSource(config);
             Connection conn = hds.getConnection()) {
            assertNotNull(conn);
        }
    }

    @SuppressWarnings("unchecked")
    private class BogusContext extends AbstractContext {
        @Override
        public Context createSubcontext(Name name) {
            return null;
        }

        @Override
        public Object lookup(String name) {
            final LightDataSource ds = new LightDataSource();
            ds.setPoolName("TestJNDI");
            return ds;
        }
    }

    @SuppressWarnings("unchecked")
    private class BogusContext2 extends AbstractContext {
        @Override
        public Context createSubcontext(Name name) {
            return null;
        }

        @Override
        public Object lookup(String name) {
            return new StubDataSource();
        }
    }

    private class BogusRef extends RefAddr {
        private static final long serialVersionUID = 1L;

        private String content;

        BogusRef(String type, String content) {
            super(type);
            this.content = content;
        }

        @Override
        public Object getContent() {
            return content;
        }
    }
}
