package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.util.ConcurrentBag;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;

/**
 * @author Brett Wooldridge
 */
public class TestConcurrentBag {
    private static LightDataSource ds;
    private static LightPool pool;

    @BeforeClass
    public static void setup() {
        LightConfig config = TestElf.newLightConfig();
        config.setMinIdle(1);
        config.setMaxPoolSize(2);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        ds = new LightDataSource(config);
        pool = TestElf.getPool(ds);
    }

    @AfterClass
    public static void teardown() {
        ds.close();
    }

    @Test
    public void testConcurrentBag() throws Exception {
        try (ConcurrentBag<PoolEntry> bag = new ConcurrentBag<>((x) -> CompletableFuture.completedFuture(Boolean.TRUE))) {
            assertEquals(0, bag.values(8).size());

            PoolEntry reserved = pool.newPoolEntry();
            bag.add(reserved);
            bag.reserve(reserved);      // reserved

            PoolEntry inuse = pool.newPoolEntry();
            bag.add(inuse);
            bag.borrow(2, MILLISECONDS); // in use

            PoolEntry notinuse = pool.newPoolEntry();
            bag.add(notinuse); // not in use

            bag.dumpState();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos, true);
            TestElf.setSlf4jTargetStream(ConcurrentBag.class, ps);

            bag.requite(reserved);

            bag.remove(notinuse);
            assertTrue(new String(baos.toByteArray()).contains("not borrowed or reserved"));

            bag.unreserve(notinuse);
            assertTrue(new String(baos.toByteArray()).contains("was not reserved"));

            bag.remove(inuse);
            bag.remove(inuse);
            assertTrue(new String(baos.toByteArray()).contains("not borrowed or reserved"));

            bag.close();
            try {
                PoolEntry bagEntry = pool.newPoolEntry();
                bag.add(bagEntry);
                assertNotEquals(bagEntry, bag.borrow(100, MILLISECONDS));
            } catch (IllegalStateException e) {
                assertTrue(new String(baos.toByteArray()).contains("ignoring add()"));
            }

            assertNotNull(notinuse.toString());
        }
    }
}
