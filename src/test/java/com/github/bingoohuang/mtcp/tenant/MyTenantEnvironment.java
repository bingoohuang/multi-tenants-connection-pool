package com.github.bingoohuang.mtcp.tenant;

import com.github.bingoohuang.mtcp.TenantEnvironment;
import com.github.bingoohuang.mtcp.TenantEnvironmentAware;
import lombok.val;

public class MyTenantEnvironment implements TenantEnvironmentAware {
    @Override public TenantEnvironment getTenantEnvironment() {
        val tenantId = MyTenantContext.getTenantId();
        return new TenantEnvironment(tenantId,
                "INSERT INTO T_CURRENT_TENANT(TENANT_ID) VALUES('" + tenantId + "')");
    }
}
