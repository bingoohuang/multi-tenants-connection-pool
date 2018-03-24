package com.github.bingoohuang.mtcp.util;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class UtilityElfTest {
    @Test
    public void testCreateRandomPoolName() {
        String randomPoolName = UtilityElf.createRandomPoolName("pool");
        assertNotNull(randomPoolName);
    }
}
