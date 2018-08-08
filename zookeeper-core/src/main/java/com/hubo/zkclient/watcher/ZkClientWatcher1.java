package com.hubo.zkclient.watcher;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.List;

public class ZkClientWatcher1 {

    private static final String CONNECT_ADDRESS="";

    private static final int SESSION_TIMEOUT=5000;

    private static final String PARENT_NODE="/zkclinet";
    
    
    public static void main(String[] args) throws Exception{
        ZkClient zkc=new ZkClient(new ZkConnection(CONNECT_ADDRESS),SESSION_TIMEOUT);
        
        //对父节点添加监听子节点变化
        zkc.subscribeChildChanges(PARENT_NODE, new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                System.out.println("parentPath:"+parentPath);
                System.out.println("currentChilds:"+currentChilds);
            }
        });
        
        Thread.sleep(3000);
        
        zkc.createPersistent(PARENT_NODE);
        Thread.sleep(1000);
        
        zkc.createPersistent(PARENT_NODE+"/c1","c1内容");
        Thread.sleep(1000);
        
        zkc.createPersistent(PARENT_NODE+"/c2","c2内容");
        Thread.sleep(1000);
        
        zkc.delete(PARENT_NODE+"c2");
        Thread.sleep(1000);
        
        zkc.deleteRecursive(PARENT_NODE);
        
        System.in.read();
    }
}
