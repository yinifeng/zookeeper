package com.hubo.curator.barrier;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Random;

public class CuratorBarrier2 {
    static final String CONNECT_ADDR="192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    static final int SESSION_TIMEOUT=6000;
    static DistributedBarrier barrier;

    public static void main(String[] args) throws Exception {
        for (int i=0;i<5;i++){
            new Thread(()->{
                try {
                    RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,10);
                    CuratorFramework cf = CuratorFrameworkFactory.builder()
                            .connectString(CONNECT_ADDR)
                            .sessionTimeoutMs(SESSION_TIMEOUT)
                            .retryPolicy(retryPolicy)
                            .build();
                    cf.start();

                    barrier = new DistributedBarrier(cf, "/barrier");
                    System.out.println(Thread.currentThread().getName()+"设置barrier！");
                    barrier.setBarrier();//设置
                    barrier.waitOnBarrier();//等待
                    System.out.println("-------------开始执行程序-------------");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            },"t"+i).start();
        }
        
        Thread.sleep(90000);
        barrier.removeBarrier();//释放
    }
}
