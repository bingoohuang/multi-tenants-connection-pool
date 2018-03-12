package com.github.bingoohuang.mtcp.util;

import lombok.Value;

@Value
public class BagWaiting {
    private final int waiting;
    private final String tenantCode;
}
