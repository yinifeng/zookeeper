package com.hubo.curator.lock;

import java.lang.annotation.Retention;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;

/**
 * JDK实现的锁 同一个JVM
 */
public class Lock1 {
    
    static ReentrantLock reentrantLock=new ReentrantLock();
    static int count=10;
    
    public static void genarNo(){
        try {
            reentrantLock.lock();
            count--;
            System.out.println(Thread.currentThread().getName()+":"+count);
        } finally {
            reentrantLock.unlock();
        }

    }

    public static void main(String[] args) throws InterruptedException {
        final CountDownLatch countDown=new CountDownLatch(1);
        for(int i=0;i<10;i++){
            new Thread(()->{
                try {
                    countDown.await();
                    genarNo();
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss|SSS");
                    System.out.println(dtf.format(LocalDateTime.now()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            },"t"+i).start();
        }
        
        Thread.sleep(50);
        countDown.countDown();
    }
    
}
