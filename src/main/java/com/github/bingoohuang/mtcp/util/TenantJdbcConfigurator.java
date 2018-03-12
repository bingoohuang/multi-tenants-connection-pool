package com.github.bingoohuang.mtcp.util;

interface TenantJdbcConfigurator {
    TenantDataSource.ConnectionConfig getConnectionConfig(String tenantCode);

    String getJdbcUrl();
}
