package com.hubo.zookeeper.cluster;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

public class ZKWatcher implements Watcher{
    /**zk变量*/
    private ZooKeeper zk;
    
    /**父节点path*/
    private static final String PARENT_PATH="/super";
    
    /**信号量设置，用于等待zookeeper连接建立之后 通知阻塞程序继续向下执行*/
    private CountDownLatch connectedSemaphore=new CountDownLatch(1);
    
    private List<String> cowaList=new CopyOnWriteArrayList<>();
    
    /**zk服务器地址*/
    private static final String CONNECTION_ADDR="";
    
    /**定义session失效时间*/
    private static final int SESSION_TIMEOUT=30000;
    
    public ZKWatcher() throws Exception {
        zk=new ZooKeeper(CONNECTION_ADDR,SESSION_TIMEOUT,this);
        System.out.println("开始连接zookeeper服务器");
        connectedSemaphore.await();
    }
    
    @Override
    public void process(WatchedEvent event) {
        System.out.println("进入 process......event = "+event);
        if(event == null){
            return;
        }

        //连接状态
        Event.KeeperState keeperState = event.getState();
        //事件类型
        Event.EventType eventType = event.getType();
        //受影响的path
        String path = event.getPath();
        System.out.println("受影响的path："+path);
        if (Event.KeeperState.SyncConnected == keeperState) {
            //成功连接上zookeeper服务器
            if (Event.EventType.None == eventType){
                System.out.println("成功连接上zookeeper服务器");
                connectedSemaphore.countDown();//通知信号，线程解除阻塞
                try {
                    if (this.zk.exists(PARENT_PATH, false) == null){
                        this.zk.create(PARENT_PATH, "root".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                    List<String> paths = this.zk.getChildren(PARENT_PATH, true);
                    for (String p:paths) {
                        System.out.println(p);
                        this.zk.exists(PARENT_PATH+"/"+p, true);
                    }
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                } 
            }//创建节点
            else if (Event.EventType.NodeCreated == eventType){
                System.out.println("节点创建");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }//更新子节点
            else if (Event.EventType.NodeChildrenChanged == eventType){
                System.out.println("子节点更新");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }//更新节点
            else if (Event.EventType.NodeDataChanged == eventType){
                System.out.println("节点数据更新");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }//删除节点
            else if (Event.EventType.NodeDeleted == eventType){
                System.out.println("节点"+path+" 被删除");
            }

        }else if (Event.KeeperState.Disconnected == keeperState){
            System.out.println("与zookeeper服务器断开连接");
        }else if (Event.KeeperState.AuthFailed == keeperState){
            System.out.println("权限检查失败");
        }else if (Event.KeeperState.Expired == keeperState){
            System.out.println("会话失效");
        }

        System.out.println("----------------------------------------");

    }
}
