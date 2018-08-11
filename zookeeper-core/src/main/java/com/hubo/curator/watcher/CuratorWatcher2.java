package com.hubo.curator.watcher;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;


/**
 * Curator框架对zookeeper节点的监听
 */
public class CuratorWatcher2 {
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

        //4.建立一个PathChildrenCache缓存,第三个参数为是否接受节点数据内容，如果为false不接受
        final PathChildrenCache cache = new PathChildrenCache(cf,PARENT_NODE, true);
        //5.在初始化的时候进行缓存监听
        cache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        cache.getListenable().addListener(new PathChildrenCacheListener() {

            /**
             * 监听子节点变更
             * 新建、修改、删除
             * @param client
             * @param event
             * @throws Exception
             */
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()){
                    case CHILD_ADDED:
                        System.out.println("CHILD_ADDED :"+event.getData().getPath());
                        break;
                    case CHILD_REMOVED:
                        System.out.println("CHILD_REMOVED :"+event.getData().getPath());
                        break;
                    case CHILD_UPDATED:
                        System.out.println("CHILD_UPDATED :"+event.getData().getPath());
                        break;
                    default:
                        break;
                }
            }
        });
        
        //这个地方休眠会抛异常
        //Thread.sleep(1000);
        //创建监听节点，监听节点不发生变化
        cf.create().forPath(PARENT_NODE,"init".getBytes());
        
        Thread.sleep(1000);
        cf.create().forPath(PARENT_NODE+"/c1", "c1内容".getBytes());
        Thread.sleep(1000);
        cf.create().forPath(PARENT_NODE+"/c2", "c2内容".getBytes());
        
        //修改子节点
        Thread.sleep(1000);
        cf.setData().forPath(PARENT_NODE+"/c1", "c1更新内容".getBytes());
        
        //删除子节点
        Thread.sleep(1000);
        cf.delete().forPath(PARENT_NODE+"/c2");
        
        //删除本身节点
        Thread.sleep(1000);
        cf.delete().deletingChildrenIfNeeded().forPath(PARENT_NODE);
        
        System.in.read();
    }
}
