package com.github.bingoohuang.mtcp.util;

import com.github.bingoohuang.mtcp.TenantCodeAware;
import lombok.val;

import java.util.concurrent.atomic.AtomicInteger;

public class BagWaiter {
    private final AtomicInteger waiters;
    private final TenantCodeAware tenantCodeAware;

    public BagWaiter(TenantCodeAware tenantCodeAware) {
        this.waiters = new AtomicInteger();
        this.tenantCodeAware = tenantCodeAware;
    }

    public BagWaiting increaseWaiting(String tenantCode) {
        val waiting = waiters.incrementAndGet();
        return new BagWaiting(waiting, tenantCode);
    }

    public String getTenantCode() {
        return tenantCodeAware != null ? tenantCodeAware.getTenantCode() : null;
    }

    public void decrementWaiting() {
        waiters.decrementAndGet();
    }

    public boolean hasWaiters() {
        return waiters.get() > 0;
    }

    public int getWaiterCount() {
        return waiters.get();
    }
}
