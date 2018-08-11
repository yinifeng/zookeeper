package com.hubo.curator.cluster;


import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

public class Test {
    
    static final String CONNECT_ADDR="192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    
    static final int SESSION_TIMEOUT=6000;
    
    static final String PARENT_NODE="/super";

    public static void main(String[] args) throws Exception {
        RetryPolicy retryPolicy=new ExponentialBackoffRetry(1000,10);

        CuratorFramework cf = CuratorFrameworkFactory.builder()
                .connectString(CONNECT_ADDR)
                .sessionTimeoutMs(SESSION_TIMEOUT)
                .retryPolicy(retryPolicy)
                .build();
        cf.start();
        
        Thread.sleep(3000);

        //System.out.println(cf.getChildren().forPath(PARENT_NODE).get(0));
        
        //4创建节点
        Thread.sleep(1000);
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(PARENT_NODE+"/c1", "c1内容".getBytes());
        Thread.sleep(1000);
        cf.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(PARENT_NODE+"/c2", "c2内容".getBytes());
        Thread.sleep(1000);
        
        
        //5.读取节点
        String ret1 = new String(cf.getData().forPath(PARENT_NODE + "/c1"));
        System.out.println(ret1);
        
        //6.修改数据
        Thread.sleep(1000);
        cf.setData().forPath(PARENT_NODE+"/c2", "修改的新c2的内容".getBytes());
        String ret2 = new String(cf.getData().forPath(PARENT_NODE + "/c2"));
        System.out.println(ret2);
        
        //7.删除节点
        Thread.sleep(1000);
        cf.delete().forPath(PARENT_NODE+"/c1");
    }
}
