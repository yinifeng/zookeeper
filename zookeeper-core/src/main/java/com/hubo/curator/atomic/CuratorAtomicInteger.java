package com.hubo.curator.atomic;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicInteger;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.retry.RetryNTimes;

public class CuratorAtomicInteger {
    static final String CONNECT_ADDR="192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    static final int SESSION_TIMEOUT=6000;
    
    
    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,10);
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .build();
        cf.start();

        DistributedAtomicInteger atomicInteger = new DistributedAtomicInteger(cf, "/atomic", new RetryNTimes(3, 1000));
        AtomicValue<Integer> value = atomicInteger.add(1);
        System.out.println(value.succeeded());
        System.out.println(value.postValue());
        System.out.println(value.preValue());
        
    }
}
