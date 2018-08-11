package com.hubo.curator.cluster;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

public class CuratorWatcher {
    public static final String PARENT_NODE="/super";
    
    public static final String CONNECT_ADDR="192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    
    public static final int SESSION_TIMEOUT=6000;
    
    public CuratorWatcher() throws Exception{
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,3);

        CuratorFramework cf = CuratorFrameworkFactory
                .newClient(CONNECT_ADDR, SESSION_TIMEOUT, SESSION_TIMEOUT, retryPolicy);
        cf.start();
        
        //4.创建跟节点
        if(cf.checkExists().forPath(PARENT_NODE) == null){
            cf.create().withMode(CreateMode.PERSISTENT).forPath(PARENT_NODE, "super init".getBytes());
        }
        
        //建立一个Cache缓存，第三个参数是否接受节点数据内容 如果为false则不接受
        PathChildrenCache cache = new PathChildrenCache(cf, PARENT_NODE, true);
        //在初始化的时候进行缓存
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        //监听子节点变化 新建，修改，删除
        cache.getListenable().addListener((client,event)->{
            switch (event.getType()) {
                case CHILD_ADDED:
                    System.out.println("CHILD_ADDED :"+event.getData().getPath());
                    System.out.println("CHILD_ADDED :"+new String(event.getData().getData()));
                    break;
                case CHILD_UPDATED:
                    System.out.println("CHILD_UPDATED :"+event.getData().getPath());
                    System.out.println("CHILD_UPDATED :"+new String(event.getData().getData()));
                    break;
                case CHILD_REMOVED:
                    System.out.println("CHILD_REMOVED :"+event.getData().getPath());
                    System.out.println("CHILD_REMOVED :"+new String(event.getData().getData()));
                    break;
                default:
                    break;
            }
        });
    }
}
