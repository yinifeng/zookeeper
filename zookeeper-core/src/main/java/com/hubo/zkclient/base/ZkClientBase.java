package com.hubo.zkclient.base;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.List;

public class ZkClientBase {
    
    private static final String CONNECT_ADDRESS="192.168.123.60:2181,192.168.123.61:2181,192.168.123.62:2181";
    
    private static final int SESSION_TIMEOUT=5000;
    
    private static final String PARENT_NODE="/zkclinet";

    public static void main(String[] args) throws Exception{
        ZkClient zkc=new ZkClient(new ZkConnection(CONNECT_ADDRESS),SESSION_TIMEOUT);
        //1.create and delete方法
        zkc.createEphemeral("/temp");//创建临时节点
        //递归创建不能携带数据
//        zkc.createPersistent(PARENT_NODE+"/c1",true);//递归创建
//        Thread.sleep(10000);
        
//        zkc.delete("/temp");
        //递归删除
//        zkc.deleteRecursive(PARENT_NODE);
        
        
        //2.设置path和data 并且读取子节点和每个节点的内容
//        zkc.createPersistent(PARENT_NODE,"1234");
//        zkc.createPersistent(PARENT_NODE+"/c1","c1内容");
//        zkc.createPersistent(PARENT_NODE+"/c2","c2内容");
//        List<String> list = zkc.getChildren(PARENT_NODE);
//        list.forEach(p->{
//            System.out.println(p);
//            String rp=PARENT_NODE+"/"+p;
//            String data = zkc.readData(rp);
//            System.out.println(String.format("节点为：%s，内容为：%s",rp,data));
//        });
        
        //3.更新和判断节点是否存在
//        zkc.writeData(PARENT_NODE+"/c1", "新内容");
//        String data = (String) zkc.readData(PARENT_NODE + "/c1");
//        System.out.println(data);
//        System.out.println(zkc.exists(PARENT_NODE+"/c1"));
        
        //递归删除/super内容
        zkc.deleteRecursive(PARENT_NODE);
        
        zkc.close();
    }
}
