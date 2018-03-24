package com.github.bingoohuang.mtcp;

import java.sql.Connection;

public interface TenantEnvironmentAware {
    String getTenantId();

    void switchTenantDatabase(Connection connection);

    void tagActive(String connectionName);
}
