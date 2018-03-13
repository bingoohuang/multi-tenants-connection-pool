package com.github.bingoohuang.mtcp;

import lombok.Value;

@Value
public class TenantEnvironment {
    private final String tcode;
    private final String switchDbSql;
}

