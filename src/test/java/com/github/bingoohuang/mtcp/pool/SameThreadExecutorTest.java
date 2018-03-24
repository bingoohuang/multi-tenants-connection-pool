package com.github.bingoohuang.mtcp.pool;

import org.junit.Test;

import javax.xml.ws.Holder;

import static org.junit.Assert.assertSame;

public class SameThreadExecutorTest {
    @Test
    public void test() {
        Holder<Thread> threadName = new Holder<>();
        SameThreadExecutor.INSTANCE.execute(() -> {
            threadName.value = Thread.currentThread();
        });

        assertSame(threadName.value, Thread.currentThread());
    }
}
