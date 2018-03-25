package com.github.bingoohuang.mtcp.tenant;

import com.github.bingoohuang.mtcp.TenantEnvironmentAware;
import lombok.SneakyThrows;
import lombok.val;

import java.sql.Connection;

public class MyTenantEnvironment implements TenantEnvironmentAware {
    @Override public String getTenantId() {
        return MyTenantContext.getTenantId();
    }

    @Override public void tagActiveConnection(int connectionSeq) {
//    Jedis jedis = new Jedis();
//    ExecutorService executor = Executors.newSingleThreadExecutor();

//        executor.submit(() -> {
//            jedis.zadd("mtcp", System.currentTimeMillis(), connectionSeq);
//        });
    }

    @SneakyThrows
    @Override public void switchTenantDatabase(Connection connection) {
        val tenantId = MyTenantContext.getTenantId();

        if (tenantId == null) return;

        try (val statement = connection.createStatement()) {
            statement.execute("INSERT INTO T_CURRENT_TENANT(TENANT_ID) VALUES('" + tenantId + "')");
        }
    }


}
