package com.hubo.curator.lock;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

/**
 * zk 实现分布式锁
 */
public class Lock2 {
    static final String CONNECT_ADDR="192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    static final int SESSION_TIMEOUT=6000;
    
    public static CuratorFramework createCuratorFramework(){
        //1.重试策略：初始时间为1s 重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000,10);
        //2.通过工厂创建连接
       return  CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .build();
        
    }

    public static void main(String[] args) throws InterruptedException {
        final CountDownLatch countDownLatch=new CountDownLatch(1);
        for(int i=0 ; i <10 ;i++){
            new Thread(()->{
                CuratorFramework cf = createCuratorFramework();
                cf.start();
                final InterProcessMutex lock=new InterProcessMutex(cf,"/lock");
                final ReentrantLock reentrantLock=new ReentrantLock();
                try {
                    countDownLatch.await();
                    lock.acquire();
                    reentrantLock.lock();
                    System.out.println(Thread.currentThread().getName()+"执行业务逻辑...");
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    try {
                        //释放
                        lock.release();
                        reentrantLock.unlock();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            },"t"+i).start();
        }
        
        Thread.sleep(2000);
        countDownLatch.countDown();
    }
}
