package com.github.bingoohuang.mtcp;

import java.sql.Connection;

public interface TenantEnvironmentAware {
    String getTenantId();
    void tagActiveConnection(int connectionSeq);
    void switchTenantDatabase(Connection connection);
}
