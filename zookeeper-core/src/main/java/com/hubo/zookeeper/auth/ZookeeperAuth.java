package com.hubo.zookeeper.auth;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * zookeeper 节点授权
 */
public class ZookeeperAuth implements Watcher{
    /**连接地址*/
    private static final String CONNECT_ADDR="192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    /**测试路劲*/
    private static final String PATH="/testAuth";
    private static final String PATH_DEL="/testAuth/delNode";
    /**认证类型*/
    private static final String AUTH_TYPE="digest";
    /**认证正确方法*/
    private static final String correctAuthentication= "123456";
    /**认证错误方法*/
    private static final String badAuthentication = "654321";
    
    private static ZooKeeper zk;
    /**计时器*/
    private AtomicInteger seq=new AtomicInteger();
    /**标识*/
    private static final String LOG_PREFIX_OF_MAIN="【Main】";
    
    private CountDownLatch connectedSemaphore=new CountDownLatch(1);
    
    
    public static void main(String[] args) throws Exception{
        String str="";
        ZookeeperAuth testAuth=new ZookeeperAuth();
        testAuth.createConnection(CONNECT_ADDR, 2000);
        List<ACL> acls=new ArrayList<>();
        for (ACL acl: ZooDefs.Ids.CREATOR_ALL_ACL){
            acls.add(acl);
        }

        try {
            zk.create(PATH, "init content".getBytes(), acls, CreateMode.PERSISTENT);
            System.out.println("使用授权key："+correctAuthentication+"创建节点："+PATH+",初始内容是：init content");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            zk.create(PATH_DEL, "will be deleted!".getBytes(), acls, CreateMode.PERSISTENT);
            System.out.println("使用授权key："+correctAuthentication+"创建节点："+PATH_DEL+",初始内容是：will be deleted!");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        //获取数据
        getDataByNoAuthentication();
        getDataByBadAuthentication();
        getDataByCorrectAuthentication();
        
        //更新数据
        updateDataByNoAuthentication();
        updateDataByBadAuthentication();
        updateDataByCorrectAuthentication();
        
        //删除节点
        deleteDataByNoAuthentication();
        deleteDataByBadAuthentication();
        deleteDataByCorrectAuthentication();
        
        Thread.sleep(1000);
        deleteParent();
        //释放连接
        testAuth.releaseConnection();
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (event == null){
            return;
        }
        
        //连接状态
        Event.KeeperState keeperState = event.getState();
        //事件类型
        Event.EventType eventType = event.getType();
        //受影响的path
        String eventPath = event.getPath();
        
        String logPrefix="【Watcher-"+this.seq.incrementAndGet()+"】";

        System.out.println(logPrefix+"收到Watcher通知");
        System.out.println(logPrefix + "连接转态：\t"+keeperState.toString());
        System.out.println(logPrefix + "事件类型：\t"+eventType.toString());
        if (Event.KeeperState.SyncConnected == keeperState){
            //成功连接zookeeper服务器
            if(Event.EventType.None == eventType){
                System.out.println(logPrefix+"成功连接zookeeper服务器");
                connectedSemaphore.countDown();
            }
        }else if (Event.KeeperState.Disconnected == keeperState){
            System.out.println(logPrefix+"与zookeeper服务器断开连接");
        }else if (Event.KeeperState.Expired == keeperState){
            System.out.println(logPrefix+"权限检查失败");
        }else if(Event.KeeperState.AuthFailed == keeperState){
            System.out.println(logPrefix+"会话失效");
        }
        System.out.println("----------------------------------------------");
    }

    /**
     * 连接zookeeper
     * @param connectString
     * @param sessionTimeout
     */
    public void createConnection(String connectString,int sessionTimeout){
        try {
            zk=new ZooKeeper(connectString,sessionTimeout,this);
            //添加节点授权
            zk.addAuthInfo(AUTH_TYPE, correctAuthentication.getBytes());
            System.out.println(LOG_PREFIX_OF_MAIN+"开始连接zookeeper服务器");
            connectedSemaphore.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭zk连接
     */
    public void releaseConnection(){
        if (zk != null){
            try {
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 获取数据采用错误的密码
     */
    public static void getDataByBadAuthentication(){
        String prefix="[使用错误的授权的信息]";
        try {
            ZooKeeper badzk=new ZooKeeper(CONNECT_ADDR,2000,null);
            //授权
            badzk.addAuthInfo(AUTH_TYPE,badAuthentication.getBytes());
            Thread.sleep(2000);
            System.out.println(prefix+"获取数据："+PATH);
            System.out.println(prefix+"成功获取数据："+badzk.getData(PATH,false,null));
        } catch (Exception e) {
            System.err.println(prefix+"获取数据失败，原因："+e.getMessage());
        }
    }

    /**
     * 获取数据，不采用密码
     */
    public static void getDataByNoAuthentication(){
        String prefix="[不使用任何的授权信息]";
        try {
            ZooKeeper nozk=new ZooKeeper(CONNECT_ADDR,2000,null);
            Thread.sleep(2000);
            System.out.println(prefix+"获取数据："+PATH);
            System.out.println(prefix+"成功获取数据："+nozk.getData(PATH,false,null));
        } catch (Exception e) {
            System.err.println(prefix+"获取数据失败，原因："+e.getMessage());
        }
    }

    /**
     * 采用的正确的密码获取数据
     */
    public static void getDataByCorrectAuthentication(){
        String prefix="[使用正确的授权信息]";
        try {
            System.out.println(prefix+"获取数据："+PATH);
            System.out.println(prefix+"成功获取数据："+zk.getData(PATH,false,null));
        } catch (Exception e) {
            System.err.println(prefix+"获取数据失败，原因："+e.getMessage());
        }
    }

    /**
     * 更新数据采用错误的密码
     */
    public static void updateDataByBadAuthentication(){
        String prefix="[使用错误的授权信息]";
        System.out.println(prefix+"更新数据："+PATH);
        try {
            ZooKeeper badzk=new ZooKeeper(CONNECT_ADDR,2000,null);
            //授权
            badzk.addAuthInfo(AUTH_TYPE,badAuthentication.getBytes());
            Thread.sleep(2000);
            Stat stat = badzk.exists(PATH, false);
            if (stat != null){
                badzk.setData(PATH, prefix.getBytes(), -1);
                System.out.println(prefix+"更新成功");
            }
        } catch (Exception e) {
            System.err.println(prefix+"更新数据失败，原因："+e.getMessage());
        }
    }

    /**
     * 更新数据，不采用密码
     */
    public static void updateDataByNoAuthentication(){
        String prefix="[不使用任何授权信息]";
        System.out.println(prefix+"更新数据："+PATH);
        try {
            ZooKeeper nozk=new ZooKeeper(CONNECT_ADDR,2000,null);
            Thread.sleep(2000);
            Stat stat = nozk.exists(PATH, false);
            if (stat != null){
                nozk.setData(PATH, prefix.getBytes(), -1);
                System.out.println(prefix+"更新成功");
            }
        } catch (Exception e) {
            System.err.println(prefix+"更新数据失败，原因："+e.getMessage());
        }
    }

    /**
     * 采用的正确的密码更新数据
     */
    public static void updateDataByCorrectAuthentication(){
        String prefix="[使用正确的授权信息]";
        System.out.println(prefix+"更新数据："+PATH);
        try {
            Stat stat = zk.exists(PATH, false);
            if (stat != null){
                zk.setData(PATH, prefix.getBytes(), -1);
                System.out.println(prefix+"更新成功");
            }
        } catch (Exception e) {
            System.err.println(prefix+"更新数据失败，原因："+e.getMessage());
        }
    }

    /**
     * 不采用密码删除节点
     */
    public static void deleteDataByNoAuthentication(){
        String prefix="[不使用任何授权信息]";
        System.out.println(prefix+"删除节点："+PATH_DEL);
        try {
            ZooKeeper nozk=new ZooKeeper(CONNECT_ADDR,2000,null);
            Thread.sleep(2000);
            Stat stat = nozk.exists(PATH_DEL, false);
            if (stat != null){
                nozk.delete(PATH_DEL, -1);
                System.out.println(prefix+"删除成功");
            }
        } catch (Exception e) {
            System.err.println(prefix+"删除失败，原因："+e.getMessage());
        }
    }

    /**
     * 采用错误的密码删除节点
     */
    public static void deleteDataByBadAuthentication(){
        String prefix="[错误的授权信息]";
        System.out.println(prefix+"删除节点："+PATH_DEL);
        try {
            ZooKeeper badzk=new ZooKeeper(CONNECT_ADDR,2000,null);
            //授权
            badzk.addAuthInfo(AUTH_TYPE,badAuthentication.getBytes());
            Thread.sleep(2000);
            Stat stat = badzk.exists(PATH_DEL, false);
            if (stat != null){
                badzk.delete(PATH_DEL, -1);
                System.out.println(prefix+"删除成功");
            }
        } catch (Exception e) {
            System.err.println(prefix+"删除失败，原因："+e.getMessage());
        }
    }

    /**
     * 采用的正确的密码删除节点
     */
    public static void deleteDataByCorrectAuthentication(){
        String prefix="[正确的授权信息]";
        System.out.println(prefix+"删除节点："+PATH_DEL);
        try {
            Stat stat = zk.exists(PATH_DEL, false);
            if (stat != null){
                zk.delete(PATH_DEL, -1);
                System.out.println(prefix+"删除成功");
            }
        } catch (Exception e) {
            System.err.println(prefix+"删除失败，原因："+e.getMessage());
        }
    }

    /**
     * 使用正确密码删除节点
     */
    public static void deleteParent(){
        try {
            Stat stat = zk.exists(PATH_DEL, false);
            if(stat == null){
                zk.delete(PATH, -1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
