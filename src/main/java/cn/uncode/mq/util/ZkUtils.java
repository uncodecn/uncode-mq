package cn.uncode.mq.util;


import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.cluster.Cluster;
import cn.uncode.mq.cluster.Group;
import cn.uncode.mq.exception.ZkNoNodeException;
import cn.uncode.mq.exception.ZkNodeExistsException;
import cn.uncode.mq.server.ServerRegister;
import cn.uncode.mq.store.zk.ZkTopicQueueReadIndex;
import cn.uncode.mq.zk.ZkClient;

/**
 * @author juny.ye
 */
public class ZkUtils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ZkUtils.class);

    public static final String ZK_MQ_BASE = "/uncodemq";

    public static void makeSurePersistentPathExists(ZkClient zkClient, String path) {
        if (!zkClient.exists(path)) {
        	try {
        		zkClient.createPersistent(path, true);
			} catch (Exception e) {
				zkClient.delete(path);
				zkClient.createPersistent(path, true);
			}
        }
    }

    /**
     * get children nodes name
     *
     * @param zkClient zkClient
     * @param path     full path
     * @return children nodes name or null while path not exist
     */
    public static List<String> getChildrenParentMayNotExist(ZkClient zkClient, String path) {
        try {
            return zkClient.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        }
    }

    public static String readData(ZkClient zkClient, String path) {
        return fromBytes(zkClient.readData(path));
    }

    public static String readDataMaybeNull(ZkClient zkClient, String path) {
        return fromBytes(zkClient.readData(path, true));
    }

//    public static void updatePersistentPath(ZkClient zkClient, String path, String data) {
//        try {
//            zkClient.writeData(path, getBytes(data));
//        } catch (ZkNoNodeException e) {
//            createParentPath(zkClient, path);
//            try {
//                zkClient.createPersistent(path, getBytes(data));
//            } catch (ZkNodeExistsException e2) {
//                zkClient.writeData(path, getBytes(data));
//            }
//        }
//    }

    private static void createParentPath(ZkClient zkClient, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            zkClient.createPersistent(parentDir, true);
        }
    }
    
    /**
     * read all broker group in the zookeeper
     *
     * @param zkClient zookeeper client
     * @return all brokers
     */
    public static void getCustomerCluster(ZkClient zkClient, String[] queueNames) {
    	if(queueNames != null && queueNames.length > 0){
    		getCluster(zkClient);
    		for(String queue : queueNames){
    			if(StringUtils.isNotBlank(queue)){
    				List<String> childrens = zkClient.getChildren(ZkTopicQueueReadIndex.ZK_INDEX + "/" + queue);
    				if(childrens != null){
    					for(String child:childrens){
        					String[] ips = child.split(":");
        					if(ips != null){
        						if(ips.length == 1){
        							Cluster.putSlave(queue, ips[0], null);
        						}else{
        							Cluster.putSlave(queue, ips[0], ips[1]);
        						}
        					}
        				}
    				}
    			}
    		}
    	}
    }
    
    
    public static void loadEmbeddedCustomerCluster(ZkClient zkClient, String host) {
		if(StringUtils.isNotBlank(host)){
			List<String> childrens = zkClient.getChildren(ZkTopicQueueReadIndex.ZK_INDEX);
			if(childrens != null){
				for(String queue:childrens){
					List<String> hosts = zkClient.getChildren(ZkTopicQueueReadIndex.ZK_INDEX + "/" + queue);
					if(hosts != null){
						for(String ht:hosts){
							if(ht.contains(host)){
								Cluster.addHostQueueName(host, queue);
							}
						}
					}
				}
			}
		}
    }

    /**
     * read all broker group in the zookeeper
     *
     * @param zkClient zookeeper client
     * @return all brokers
     */
    public static void getCluster(ZkClient zkClient) {
    	List<String> allGroupNames = ZkUtils.getChildrenParentMayNotExist(zkClient, ServerRegister.ZK_BROKER_GROUP);
        if (allGroupNames != null) {
        	Cluster.clear();
        	LOGGER.info("read all broker group count: " + allGroupNames.size());
        	List<Group> allGroup = new ArrayList<Group>();
        	Map<String, String> slaveIp = new HashMap<>();
            for (String group : allGroupNames) {
                String jsonGroup = ZkUtils.readData(zkClient, ServerRegister.ZK_BROKER_GROUP + "/" + group);
                if(StringUtils.isNotBlank(jsonGroup)){
                	Group groupObj = DataUtils.json2BrokerGroup(jsonGroup);
                	allGroup.add(groupObj);
                	if(groupObj.getSlaveOf() != null){
                		slaveIp.put(groupObj.getSlaveOf().getHost(), groupObj.getMaster().getHost());
                	}
                	LOGGER.info("Loading Broker Group " + group);
                }
            }
            List<Group> noSlave = new ArrayList<Group>();
            for(Group group:allGroup){
            	if(slaveIp.containsKey(group.getMaster().getHost())){
            		group.getMaster().setShost(slaveIp.get(group.getMaster().getHost()));
            		Cluster.addGroup(group);
            	}else{
            		noSlave.add(group);
            	}
            }
            if(noSlave.size() > 0){
            	Cluster.addGroups(noSlave);
            }
        }
    }

//    public static void deletePath(ZkClient zkClient, String path) {
//        try {
//            zkClient.delete(path);
//        } catch (ZkNoNodeException e) {
//        }
//    }

    /**
     * Create an ephemeral node with the given path and data. Create parents if necessary.
     */
    public static void createEphemeralPath(ZkClient zkClient, String path, String data) {
        try {
            zkClient.createEphemeralSequential(path, getBytes(data));
        } catch (ZkNoNodeException e) {
            createParentPath(zkClient, path);
            zkClient.createEphemeralSequential(path, getBytes(data));
        }
    }

    public static void createEphemeralPathExpectConflict(ZkClient zkClient, String path, String data) {
        try {
            createEphemeralPath(zkClient, path, data);
        } catch (ZkNodeExistsException e) {
            //this can happend when there is connection loss;
            //make sure the data is what we intend to write
            String storedData = null;
            try {
                storedData = readData(zkClient, path);
            } catch (ZkNoNodeException e2) {
                //ignore
            }
            if (storedData == null || !storedData.equals(data)) {
                throw new ZkNodeExistsException("conflict in " + path + " data: " + data + " stored data: " + storedData);
            }
            //
            //otherwise, the creation succeeded, return normally
        }
    }
    
    public static String fromBytes(byte[] b) {
        return fromBytes(b, "UTF-8");
    }

    public static String fromBytes(byte[] b, String encoding) {
        if (b == null) return null;
        try {
            return new String(b, encoding);
        } catch (UnsupportedEncodingException e) {
            return new String(b);
        }
    }

    public static byte[] getBytes(String s) {
        return getBytes(s, "UTF-8");
    }

    public static byte[] getBytes(String s, String encoding) {
        if (s == null) return null;
        try {
            return s.getBytes(encoding);
        } catch (UnsupportedEncodingException e) {
            return s.getBytes();
        }
    }
    
    
    public static void printTree(ZooKeeper zk,String path,Writer writer,String lineSplitChar) throws Exception{
 	   String[] list = getTree(zk,path);
 	   Stat stat = new Stat();
 	   for(String name:list){
 		   byte[] value = zk.getData(name, false, stat);
 		   if(value == null){
 			   writer.write(name + lineSplitChar);
 		   }else{
 			   writer.write(name+"[v."+ stat.getVersion() +"]"+"["+ valueDisplay(value) +"]"  + lineSplitChar);
 		   }
 	   }
    }
    
    private static String valueDisplay(byte[] value){
    	String MAGIC = "umqv.1.0";
    	StringBuilder sb = new StringBuilder(MAGIC);
    	if(value != null && value.length > 0){
    		ByteBuffer readIndex = ByteBuffer.wrap(value);
    		byte[] bytes = new byte[8];
    		readIndex.get(bytes, 0, 8);
    		if (!MAGIC.equals(new String(bytes))) {
    			sb.append(new String(value));
    		}else{
    			sb.append("[").append("readNum:").append(readIndex.getInt())
        		.append(",readPosition:").append(readIndex.getInt())
        		.append(",readCounter:").append(readIndex.getInt())
        		.append(",writeNum:").append(readIndex.getInt())
        		.append(",writePosition:").append(readIndex.getInt())
        		.append(",writeCounter:").append(readIndex.getInt());
    		}
    		
    	}
		return sb.toString();
    }
    
    public static String[] getTree(ZooKeeper zk,String path) throws Exception{
 	   if(zk.exists(path, false) == null){
 		   return new String[0];
 	   }
 	   List<String> dealList = new ArrayList<String>();
 	   dealList.add(path);
 	   int index =0;
 	   while(index < dealList.size()){
 		   String tempPath = dealList.get(index);
 		   List<String> children = zk.getChildren(tempPath, false);
 		   if(tempPath.equalsIgnoreCase("/") == false){
 			   tempPath = tempPath +"/";
 		   }
 		   Collections.sort(children);
 		   for(int i = children.size() -1;i>=0;i--){
 			   dealList.add(index+1, tempPath + children.get(i));
 		   }
 		   index++;
 	   }
 	   return (String[])dealList.toArray(new String[0]);
    }
    
    public static void deleteTree(ZooKeeper zk,String path) throws Exception{
 	   String[] list = getTree(zk,path);
 	   for(int i= list.length -1;i>=0; i--){
 		   zk.delete(list[i],-1);
 	   }
    }
    
    public static void main(String[] args){
		try {
			ZooKeeper zk = new ZooKeeper("192.168.1.14:2181", 3000, null);
			zk.addAuthInfo("digest", "admin:password".getBytes());
			//deleteTree(zk, "/uncodemq/index/test");
			StringWriter writer = new StringWriter();
			printTree(zk, "/", writer, "\n");
			System.out.println(writer.getBuffer().toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
    }
}
