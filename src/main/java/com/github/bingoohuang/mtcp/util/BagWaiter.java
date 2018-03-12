package com.github.bingoohuang.mtcp.util;

import com.github.bingoohuang.mtcp.TenantCodeAware;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import lombok.val;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkNotNull;

public class BagWaiter {
    private final AtomicInteger waiters;
    private final Multiset<String> tenantWaiters;
    private final TenantCodeAware tenantCodeAware;

    public BagWaiter(TenantCodeAware tenantCodeAware) {
        this.waiters = new AtomicInteger();
        this.tenantCodeAware = tenantCodeAware;
        this.tenantWaiters = tenantCodeAware != null ? ConcurrentHashMultiset.create() : null;
    }

    public BagWaiting increaseWaiting(String tenantCode) {
        val waiting = waiters.incrementAndGet();
        if (tenantCode != null) {
            tenantWaiters.add(tenantCode);
        }

        return new BagWaiting(waiting, tenantCode);
    }

    public String getTenantCode() {
        return tenantCodeAware != null ? checkNotNull(tenantCodeAware.getTenantCode()) : null;
    }

    public void decrementWaiting(BagWaiting bagWaiting) {
        waiters.decrementAndGet();
        if (bagWaiting.getTenantCode() != null) {
            tenantWaiters.remove(bagWaiting.getTenantCode());
        }
    }

    public boolean hasWaiters() {
        return waiters.get() > 0;
    }

    public int getWaiterCount() {
        return waiters.get();
    }

    public String getWaitingTenantCode() {
        if (tenantCodeAware == null) return null;

        val waitingTenantCodes = tenantWaiters.elementSet();
        return waitingTenantCodes.isEmpty() ? null : waitingTenantCodes.iterator().next();
    }
}
