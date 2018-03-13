package com.github.bingoohuang.mtcp.tenant;

public class MyTenantContext {
    public static ThreadLocal<String> threadLocal = new InheritableThreadLocal<>();

    public static void setTenantId(String tenantId) {
        threadLocal.set(tenantId);
    }

    public static String getTenantId() {
        return threadLocal.get();
    }

    public static void clearTenantId() {
        threadLocal.set(null);
    }
}
