package com.hubo.curator.watcher;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;


/**
 * Curator框架对zookeeper节点的监听
 */
public class CuratorWatcher1 {
    private static final String CONNECT_ADDR = "192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    private static final int SESSION_TIMEOUT = 6000;
    
    private static final String PARENT_NODE="/super";

    public static void main(String[] args) throws Exception {
        //1.重试策略；初始时间为1s 重试10次
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 10);
        //2.通过工厂创建连接
        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .build();
        //3.建立连接
        cf.start();

        //4.建立一个Cache缓存
        final NodeCache cache = new NodeCache(cf,PARENT_NODE, false);
        cache.start(true);
        cache.getListenable().addListener(new NodeCacheListener() {

            /**
             * 触发事件为创建节点和更新节点，
             * 在删除节点的时候并也会触发此操作
             * @throws Exception
             */
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("路劲为："+cache.getCurrentData().getPath());
                System.out.println("数据为："+new String(cache.getCurrentData().getData()));
                System.out.println("转态为："+cache.getCurrentData().getStat());
                System.out.println("--------------------------------------");
            }
        });
        
        
        Thread.sleep(1000);
        cf.create().forPath(PARENT_NODE,"123".getBytes());
        
        Thread.sleep(1000);
        cf.setData().forPath(PARENT_NODE, "456".getBytes());
        
        Thread.sleep(1000);
        //DELETE也触发
        cf.delete().forPath(PARENT_NODE);
        
        System.in.read();
    }
}
