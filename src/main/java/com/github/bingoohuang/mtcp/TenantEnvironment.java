package com.github.bingoohuang.mtcp;

import lombok.Value;

@Value
public class TenantEnvironment {
    private final String tenantId;
    private final String switchDbSql;
}

