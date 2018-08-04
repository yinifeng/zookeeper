package com.hubo.zookeeper.watcher;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * zookeeper watcher
 * 本类就是一个Watcher类
 */
public class ZookeeperWatcher implements Watcher{
    /** 定义原子变量 */
    private AtomicInteger seq=new AtomicInteger();
    /** 定义session失效时间 */
    private static final int SESSION_TIMEOUT=10000;
    /** zookeeper服务器地址 */
    private static final String CONNECTION_ADDR="192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    /** zk父节点设置 */
    private static final String PARENT_NODE="/fu";
    /** zk子节点设置 */
    private static final String CHILDREN_NODE="/fu/zi";
    /** 进入标识 */
    private static final String LOG_PREFIX_OF_MAIN = "【Main】";
    /** zk变量 */
    private ZooKeeper zk;
    /** 用于等待zk连接建立之后的信号，通知阻塞程序继续向下执行 */
    private CountDownLatch connectedSemaphore=new CountDownLatch(1);


    /**
     * 创建zk连接
     * @param connectAddr 连接地址
     * @param sessionTimeout session超时时间
     */
    public void createConnection(String connectAddr,int sessionTimeout){
        try {
            //this表示当前对象进行传递到其中
            zk=new ZooKeeper(connectAddr,sessionTimeout,this);
            System.out.println(LOG_PREFIX_OF_MAIN+"开始连接zk服务器");
            connectedSemaphore.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 关闭ZK连接
     */
    public void releaseConnection(){
        if (zk !=null){
            try {
                this.zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建节点
     * @param path 节点路劲
     * @param data 节点数据
     * @param needWatch
     * @return
     */
    public boolean createNode(String path,String data,boolean needWatch){
        try {
            //设置监控(由于zookeeper的监控都是一次性的所以 每次必须设置监控)
            this.zk.exists(path,needWatch);
            System.out.println(LOG_PREFIX_OF_MAIN + "节点创建成功，Path：" +
                                this.zk.create(path,/**节点路劲*/
                                        /**节点数据*/
                                        data.getBytes(),
                                        /**所有可见*/
                                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                        /**永久存储*/
                                        CreateMode.PERSISTENT) + "，content：" +data);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 读取指定节点数据内容
     * @param path 节点路劲
     * @param needWatch
     * @return
     */
    public String readNodeData(String path,boolean needWatch){
        try {
            System.out.println("读取数据操作...");
            return new String(this.zk.getData(path,needWatch,null));
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    /**
     * 更新节点数据
     * @return
     */
    public boolean writeData(String path, String data) {
        try {
            System.out.println(LOG_PREFIX_OF_MAIN + "更新数据成功，path：" + path + "，stat：" +
                    this.zk.setData(path, data.getBytes(), -1));
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * 删除指定节点
     * @param path 节点路劲
     */
    public void deleteNode(String path){
        try {
            this.zk.delete(path,-1);
            System.out.println(LOG_PREFIX_OF_MAIN+"删除节点成功，path："+path);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断指定节点是否存在
     * @param path 节点路劲
     * @param needWatch
     * @return
     */
    public Stat exists(String path,boolean needWatch){
        try {
            return this.zk.exists(path,needWatch);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取子节点
     * @param path
     * @param needWatch
     * @return
     */
    public List<String> getChildren(String path,boolean needWatch){
        try {
            System.out.println("读取子节点操作...");
            return this.zk.getChildren(path,needWatch);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 删除所有节点
     * @param needWatch
     */
    public void deleteAllTestPath(boolean needWatch){
        if(this.exists(CHILDREN_NODE,needWatch) !=null){
            this.deleteNode(CHILDREN_NODE);
        }
        if(this.exists(PARENT_NODE,needWatch) !=null){
            this.deleteNode(PARENT_NODE);
        }
    }

    /**
     * 收到来自Server的Watcher通知
     * @param event
     */
    @Override
    public void process(WatchedEvent event) {
        System.out.println("进入 process......event = "+event);

        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        if(event == null){
            return;
        }
        
        //连接状态
        Event.KeeperState keeperState = event.getState();
        //事件类型
        Event.EventType eventType = event.getType();
        //受影响的path
        String path = event.getPath();
        //原子对象seq 记录进入process的次数
        String logPrefix="【Watcher-"+this.seq.incrementAndGet()+"】";
        System.out.println(logPrefix+"收到Watcher通知");
        System.out.println(logPrefix+"连接状态：\t"+keeperState.toString());
        System.out.println(logPrefix+"事件类型：\t"+eventType.toString());
        
        if (Event.KeeperState.SyncConnected == keeperState) {
            //成功连接上zookeeper服务器
            if (Event.EventType.None == eventType){
                System.out.println(logPrefix+"成功连接上zookeeper服务器");
                connectedSemaphore.countDown();//通知信号，线程解除阻塞
            }//创建节点
            else if (Event.EventType.NodeCreated == eventType){
                System.out.println(logPrefix+"节点创建");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }//更新子节点
            else if (Event.EventType.NodeChildrenChanged == eventType){
                System.out.println(logPrefix+"子节点更新");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }//更新节点
            else if (Event.EventType.NodeDataChanged == eventType){
                System.out.println(logPrefix+"节点数据更新");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }//删除节点
            else if (Event.EventType.NodeDeleted == eventType){
                System.out.println(logPrefix+"节点"+path+" 被删除");
            }else;
            
        }else if (Event.KeeperState.Disconnected == keeperState){
            System.out.println(logPrefix+"与zookeeper服务器断开连接");
        }else if (Event.KeeperState.AuthFailed == keeperState){
            System.out.println(logPrefix+"权限检查失败");
        }else if (Event.KeeperState.Expired == keeperState){
            System.out.println(logPrefix+"会话失效");
        }else{
            
        }

        System.out.println("----------------------------------------");

    }

    /**
     * 测试zookeeper监控
     * 主要测试watch功能
     * @param args
     */
    public static void main(String[] args) throws Exception {
        //建立watcher
        //当前客户端可以称为一个watcher观察者角色
        ZookeeperWatcher zkWatcher=new ZookeeperWatcher();
        //创建连接
        zkWatcher.createConnection(CONNECTION_ADDR,SESSION_TIMEOUT);
        System.out.println(zkWatcher.zk.toString());
        
        Thread.sleep(1000);
        
        //清理节点
        zkWatcher.deleteAllTestPath(false);
        
        //-----------------------第一步：创建父节点 /parent --------------//
        if (zkWatcher.createNode(PARENT_NODE,System.currentTimeMillis()+"",true)) {
            Thread.sleep(1000);
            
            //---------------第二部：读取节点 /parent 和 读取/parent 节点下的子节点 的区别-----------//
            //读取数据
            zkWatcher.readNodeData(PARENT_NODE,true);
            
            //读取子节点
            zkWatcher.getChildren(PARENT_NODE,true);
            
            //更新数据
            zkWatcher.writeData(PARENT_NODE,System.currentTimeMillis()+"");
            
            Thread.sleep(1000);
            //创建子节点
            zkWatcher.createNode(CHILDREN_NODE,System.currentTimeMillis()+"",true);
            
            //----------第三步：创建子节点触发---------//
            //zkWatcher.createNode(CHILDREN_NODE+"/c1",System.currentTimeMillis()+"",true);
            //zkWatcher.createNode(CHILDREN_NODE+"/c1/c2",System.currentTimeMillis()+"",true);
            
            //---------第四步：更新子节点数据的触发--------------//
            //在修改之前，我们需要watch一下这个节点
            Thread.sleep(1000);
            zkWatcher.readNodeData(CHILDREN_NODE,true);
            zkWatcher.writeData(CHILDREN_NODE,System.currentTimeMillis()+"");
            
        }
        Thread.sleep(10000);
        //清理节点
        //zkWatcher.deleteAllTestPath(false);
        
        Thread.sleep(10000);
        zkWatcher.releaseConnection();
        
    }
}
