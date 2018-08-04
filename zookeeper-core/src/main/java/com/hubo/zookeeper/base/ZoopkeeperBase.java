package com.hubo.zookeeper.base;

import org.apache.log4j.ConsoleAppender;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 原生API
 */
public class ZoopkeeperBase {
    private static Logger logger = LoggerFactory.getLogger(ZoopkeeperBase.class);
    //zookeeper连接地址
    private static final String CONNECT_ADDR = "192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    
    /** session超时时间 */
    private static final int SESSION_OUTTIME=5000;//ms
    /** 阻塞程序执行，用于等待zookeeper连接成功，发送成功信号 */
    private static final CountDownLatch connectedSemaphore =new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        
        ZooKeeper zk= new ZooKeeper(CONNECT_ADDR, SESSION_OUTTIME, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //获取事件的状态
                Event.KeeperState state = watchedEvent.getState();
                //获取事件的类型
                Event.EventType type = watchedEvent.getType();
                //如果是建立连接
                if (state == Event.KeeperState.SyncConnected) {
                    if (Event.EventType.None == type) {
                        //如果建立连接成功，则发送信号量，让后续阻塞程序向下执行
                        connectedSemaphore.countDown();
                        logger.info("zk 建立连接......");
                    }
                }
            }
        });
        
        logger.info("阻塞创建zookeeper实例！！！");
        //进行阻塞，等待zookeeper创建完成，创建zk是一个异步的过程
        connectedSemaphore.await();
        
        //创建持久化的父节点
        //如果节点存在，再创建会报节点已存在异常
        //logger.info(createNode(zk,"/testRoot","hubo"));
        //logger.info(createNode(zk,"/testRoot/a1","111"));
        //logger.info(createNode(zk,"/testRoot/a2","222"));
        //logger.info(createNode(zk,"/testRoot/a3","333"));
        
        
        //创建临时子节点
        //session关闭节点立即消失
        //不允许创建没有父节点的子节点
        //logger.info(createNode(zk,"/testRoot1/children","childern data"));
        //Thread.sleep(10000);
        
        //deleteAsyncNode(zk,"/testRoot");
        //Thread.sleep(10000);
        
        //logger.info(getNodeData(zk,"/testRoot"));
        
        modifyNodeData(zk,"/testRoot/a1","a111");

        List<String> nodes = getChildrenNode(zk, "/testRoot");
        nodes.forEach((e) ->{
            try {
                System.out.println(getNodeData(zk,"/testRoot/"+e));
            } catch (Exception e1) {
                e1.printStackTrace();
            } 
        });

        logger.info("{}",existsNode(zk, "/testRoot"));


        zk.close();
        
    }

    /**
     * 创建节点
     * 
     * 创建持久化的父节点
     * 如果节点存在，再创建会报节点已存在异常
     * 如果父节点不存在，子节点无法创建
     * 
     * @param zk
     * @param nodePath
     * @param nodeData
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static String createNode(ZooKeeper zk,String nodePath,String nodeData) throws KeeperException, InterruptedException {
        return zk.create(nodePath, nodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    /**
     * 创建临时子节点
     * 
     * 临时节点 session关闭节点立即消失
     * 不允许创建没有父节点的子节点
     * @param zk
     * @param nodePath
     * @param nodeData
     * @return
     */
    public static String createTmpChildrenNode(ZooKeeper zk,String nodePath,String nodeData) throws KeeperException, InterruptedException {
        return zk.create(nodePath, nodeData.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    }


    /**
     * 获取节点的数据
     * @param zk
     * @param nodePath
     * @return
     */
    public static String getNodeData(ZooKeeper zk,String nodePath) throws KeeperException, InterruptedException {
        byte[] data = zk.getData(nodePath, false, null);
        return new String (data);
    }

    /**
     * 修改节点的值
     * @param zk
     * @param nodePath
     * @param nodeData
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void modifyNodeData(ZooKeeper zk,String nodePath,String nodeData) throws KeeperException, InterruptedException {
        Stat result = zk.setData(nodePath, nodeData.getBytes(), -1);
    }

    /**
     * 判断节点是否存在
     * @param zk
     * @param nodePath
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static boolean existsNode(ZooKeeper zk,String nodePath) throws KeeperException, InterruptedException {
        Stat node = zk.exists(nodePath, false);
        logger.info(node.toString());
        return node != null ? true : false;
    }
    /**
     * 获取子节点
     * 只获取当前节点下的节点，无法递归获取
     * 
     * @param zk
     * @param nodePath
     * @return
     */
    public static List<String> getChildrenNode(ZooKeeper zk, String nodePath) throws KeeperException, InterruptedException {
        return zk.getChildren(nodePath,false);
    }


    /**
     * 异步删除某个节点
     * 
     * 如果存在子节点，无法删除父节点
     * 
     * @param zk
     * @param nodePath
     */
    public static void deleteAsyncNode(ZooKeeper zk,String nodePath){
        
        // version:-1 跳过版本检查
        zk.delete(nodePath, -1, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("deleteAsyncNode:{}",rc);
                logger.info("deleteAsyncNode:{}",path);
                logger.info("deleteAsyncNode:{}",ctx);
            }
        },"传入回调函数的对象，可以是任意对象");
    }
    
}
