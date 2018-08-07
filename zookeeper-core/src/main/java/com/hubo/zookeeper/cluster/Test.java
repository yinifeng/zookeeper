package com.hubo.zookeeper.cluster;

import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

/**
 * 测试Clinet1 和 Client2 监听/super 节点的变化
 * 
 * 当这个服务操作 /super 节点的时候 ，Client1 和 Client2 就会监听相应的事件做不同的事情
 * 
 * 此需求主要模拟zookeeper管理配置 类似发布订阅功能
 */
public class Test {
    private static  final String CONNECT_ADDR="192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    
    private static final int SESSION_TIMEOUT=2000;//ms
    
    private static CountDownLatch countDownLatch=new CountDownLatch(1);
    
    
    public static void main(String[] args) throws Exception{
        ZooKeeper zk=new ZooKeeper(CONNECT_ADDR, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                //获取事件状态
                Event.KeeperState state = event.getState();
                Event.EventType type = event.getType();
                if (Event.KeeperState.SyncConnected == state){
                    if (Event.EventType.None == type){
                        //如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
                        countDownLatch.countDown();
                        System.out.println("zk 建立连接。。。");
                    }
                }
            }
        }); 
        //进行阻塞
        countDownLatch.await();
        //创建子节点
//        zk.create("/super/c1","c1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        zk.create("/super/c2","c2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        zk.create("/super/c3","c3".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        zk.create("/super/c4","c4".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        zk.create("/super/c44","c44".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        
        //获取未被监听的节点
        byte[] data = zk.getData("/testRoot", false, null);
        System.out.println(new String(data));
        System.out.println(zk.getChildren("/testRoot", false));
        
        
        //修改节点的值
//        zk.setData("/super/c1", "modify c1".getBytes(), -1);
//        zk.setData("/super/c2", "modify c2".getBytes(), -1);
//        byte[] data = zk.getData("/super/c2", false, null);
//        System.out.println(new String(data));
        
        //判断节点是否存在
//        System.out.println(zk.exists("/super/c3", false));
        //删除节点
//        zk.delete("/super/c3", -1);

        zk.close();;
    }
}
