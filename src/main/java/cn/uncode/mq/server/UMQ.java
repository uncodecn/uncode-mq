package cn.uncode.mq.server;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.server.handlers.FetchRequestHandler;
import cn.uncode.mq.server.handlers.ProducerRequestHandler;
import cn.uncode.mq.server.handlers.ReplicaRequestHandler;
import cn.uncode.mq.store.TopicQueuePool;

public class UMQ {
	
	private static final String PARAM_KEY_PORT = "-p";
	private static final String PARAM_KEY_REPLICA = "-replica";
	private static final String PARAM_KEY_LOG_DIR = "-dir";
	private static final String PARAM_KEY_HOST = "-h";
	private static final String PARAM_KEY_CFG = "-cfg";
	private static final String PARAM_KEY_INFO = "-info";


    public static void main(String[] args) {
    	String info = null;
    	int port = 9999;
    	String replica = null;
    	String dir = null;
    	String host = null;
    	String cfg = null;
    	try {
	        int argsSize = args.length;
	        if (argsSize > 0) {
	        	if(argsSize % 2 != 0){
	                System.out.println("参数数量不对");
	                System.exit(1);
	        	}
	        	int i = 0;
	        	while(i < argsSize){
	        		if(StringUtils.isBlank(args[i]) || StringUtils.isBlank(args[i+1])){
	        			continue;
	        		}
	        		if(PARAM_KEY_PORT.equals(args[i].toLowerCase())){
	        			port = Integer.valueOf(args[i+1].trim());
	        		}else if(PARAM_KEY_REPLICA.equals(args[i].toLowerCase())){
	        			replica = String.valueOf(args[i+1].trim());
	        		}else if(PARAM_KEY_LOG_DIR.equals(args[i].toLowerCase())){
	        			dir = String.valueOf(args[i+1].trim());
	        		}else if(PARAM_KEY_HOST.equals(args[i].toLowerCase())){
	        			host = String.valueOf(args[i+1].trim());
	        		}else if(PARAM_KEY_CFG.equals(args[i].toLowerCase())){
	        			cfg = String.valueOf(args[i+1].trim());
	        		}else if(PARAM_KEY_INFO.equals(args[i].toLowerCase())){
	        			info = String.valueOf(args[i+1].trim());
	        		}
	        		i += 2;
	        	}
	        	ServerConfig config = null;
		        if(StringUtils.isNotBlank(cfg)){
		        	config = new ServerConfig(cfg);
		        }else{
		        	Properties cfgpro = new Properties();
		        	cfgpro.setProperty("host", host);
		        	cfgpro.setProperty("port", port+"");
			        if(StringUtils.isNotBlank(replica)){
			        	cfgpro.setProperty("replica.host", replica);
			        }
			        if(StringUtils.isNotBlank(dir)){
			        	cfgpro.setProperty("log.dir", dir);
			        }
			        config = new ServerConfig(cfgpro);
		        }
	        	if(StringUtils.isNotBlank(info)){
	        		StringBuilder sb  = new StringBuilder("==============================umqv.1.0===============================");
	        		sb.append("\n");
	        		if("all".equals(info)){
	        			sb.append(displayIndex(config, "master"));
	        			sb.append(displayIndex(config, "slave"));
	        			if(config.getEnableZookeeper()){
	        				sb.append("zk:");
	        				ZooKeeper zk = null;
							try {
								zk = new ZooKeeper(config.getZkConnect(), 3000, new Watcher() {
				                    public void process(WatchedEvent event) {
				                    	//noting
				                    }
				                });
								String uandp = config.getZkUsername() + ":" + config.getZkPassword();
								zk.addAuthInfo("digest", uandp.getBytes());
								StringWriter writer = new StringWriter();
								printTree(zk, "/uncodemq", writer, "\n");
								sb.append(writer.getBuffer().toString());
							} catch (Exception e) {
								e.printStackTrace();
							} finally{
								zk.close();
							}
	        			}
	        		}else if("master".equals(info)){
	        			sb.append(displayIndex(config, "master"));
	        		}else if("slave".equals(info)){
	        			sb.append(displayIndex(config, "slave"));
	        		}else if("zk".equals(info)){
	        			if(config.getEnableZookeeper()){
	        				sb.append("zk:");
	        				ZooKeeper zk = null;
							try {
								zk = new ZooKeeper(config.getZkConnect(), 3000, new Watcher() {
				                    public void process(WatchedEvent event) {
				                    	//noting
				                    }
				                });
								String uandp = config.getZkUsername() + ":" + config.getZkPassword();
								zk.addAuthInfo("digest", uandp.getBytes());
								StringWriter writer = new StringWriter();
								printTree(zk, "/uncodemq", writer, "\n");
								sb.append(writer.getBuffer().toString());
							} catch (Exception e) {
								e.printStackTrace();
							} finally{
								zk.close();
							}
	        			}
	        		}else if("zkdel".equals(info)){
	        			if(config.getEnableZookeeper()){
	        				sb.append("zk:");
	        				ZooKeeper zk = null;
							try {
								zk = new ZooKeeper(config.getZkConnect(), 3000, new Watcher() {
				                    public void process(WatchedEvent event) {
				                    	//noting
				                    }
				                });
								String uandp = config.getZkUsername() + ":" + config.getZkPassword();
								zk.addAuthInfo("digest", uandp.getBytes());
								deleteTree(zk, "/uncodemq/index");
								StringWriter writer = new StringWriter();
								printTree(zk, "/uncodemq", writer, "\n");
								sb.append(writer.getBuffer().toString());
							} catch (Exception e) {
								e.printStackTrace();
							} finally{
								zk.close();
							}
	        			}
	        		}
	        		sb.append("\n").append("==============================umqv.1.0===============================");
	        		System.out.println(sb.toString());
	        	}else{
	        		NettyServer nettyServer = new NettyServer();
			        nettyServer.start(config);
					nettyServer.registerHandler(RequestHandler.FETCH, new FetchRequestHandler());
					nettyServer.registerHandler(RequestHandler.PRODUCER, new ProducerRequestHandler());
					nettyServer.registerHandler(RequestHandler.REPLICA, new ReplicaRequestHandler(config));
					nettyServer.waitForClose();
	        	}
	        }
		} catch (InterruptedException e) {
			System.err.println(e);
		}
    }
    
    private static void deleteTree(ZooKeeper zk, String path) throws Exception {
		String[] list = getTree(zk, path);
		for (int i = list.length - 1; i >= 0; i--) {
			zk.delete(list[i], -1);
		}
	}
    
    private static void printTree(ZooKeeper zk, String path, Writer writer, String lineSplitChar) throws Exception {
		String[] list = getTree(zk, path);
		Stat stat = new Stat();
		for (String name : list) {
			byte[] value = zk.getData(name, false, stat);
			if (value == null) {
				writer.write(name + lineSplitChar);
			} else {
				writer.write(name + "[v." + stat.getVersion() + "]" + "[" + valueDisplay(value) + "]" + lineSplitChar);
			}
		}
	}
    
    private static String valueDisplay(byte[] value){
    	String MAGIC = "umqv.1.0";
    	StringBuilder sb  = new StringBuilder();
    	if(value != null && value.length > 0){
    		ByteBuffer readIndex = ByteBuffer.wrap(value);
    		readIndex.position(0);
    		byte[] bytes = new byte[8];
    		readIndex.get(bytes, 0, 8);
    		if (!MAGIC.equals(new String(bytes))) {
    		    sb.append(new String(value));
    		}else{
    			sb.append(MAGIC).append("=>").append("readNum:").append(readIndex.getInt())
    			.append(",readPosition:").append(readIndex.getInt())
    			.append(",readCounter:").append(readIndex.getInt())
    			.append(",writeNum:").append(readIndex.getInt())
    			.append(",writePosition:").append(readIndex.getInt())
    			.append(",writeCounter:").append(readIndex.getInt());
    		}
    	}
		return sb.toString();
    }

	private static String[] getTree(ZooKeeper zk, String path) throws Exception {
		if (zk.exists(path, false) == null) {
			return new String[0];
		}
		List<String> dealList = new ArrayList<String>();
		dealList.add(path);
		int index = 0;
		while (index < dealList.size()) {
			String tempPath = dealList.get(index);
			List<String> children = zk.getChildren(tempPath, false);
			if (tempPath.equalsIgnoreCase("/") == false) {
				tempPath = tempPath + "/";
			}
			Collections.sort(children);
			for (int i = children.size() - 1; i >= 0; i--) {
				dealList.add(index + 1, tempPath + children.get(i));
			}
			index++;
		}
		return (String[]) dealList.toArray(new String[0]);
	}
    
    
    private static String displayIndex(ServerConfig cfg, String ms){
    	StringBuilder sb  = new StringBuilder(ms+":");
    	sb.append("\n");
		String filePath = cfg.getDataDir();
		if("master".equals(ms)){
			if (StringUtils.isBlank(filePath)) {
				filePath = TopicQueuePool.DEFAULT_DATA_PATH;
			}
		}else if("slave".equals(ms)){
			if (StringUtils.isBlank(filePath)) {
				filePath = TopicQueuePool.DEFAULT_DATA_PATH;
			}
			filePath += TopicQueuePool.DATA_BACKUP_PATH;
		}
		File fileDir = new File(filePath);
		if (fileDir.exists() && fileDir.isDirectory() && fileDir.canRead()) {
			File[] indexFiles = fileDir.listFiles(new FilenameFilter() {
	            @Override
	            public boolean accept(File dir, String name) {
	            	return name.startsWith("tindex") && name.endsWith(TopicQueuePool.INDEX_FILE_SUFFIX);
	            }
	        });
	        if (ArrayUtils.isNotEmpty(indexFiles)) {
	            for (File indexFile : indexFiles) {
	            	String queueIndex = readIndexFile(indexFile);
	            	if(StringUtils.isNotBlank(queueIndex)){
	            		sb.append(queueIndex).append("\n");
	            	}
	            }
	        }
		}
    	return sb.toString();
    }
    
    private static String readIndexFile(File indexFile){
        if (indexFile != null && indexFile.exists()) {
        	RandomAccessFile indexf = null;
        	try {
        		indexf = new RandomAccessFile(indexFile, "rw");
        		String queueName = TopicQueuePool.parseQueueName(indexFile.getName());
                byte[] bytes = new byte[8];
                indexf.read(bytes, 0, 8);
                String MAGIC = "umqv.1.0";
                StringBuilder sb  = new StringBuilder();
                if (!MAGIC.equals(new String(bytes))) {
                    //nothting
                }else{
                	sb.append(queueName).append("=>").append("readNum:").append(indexf.readInt())
        			.append(",readPosition:").append(indexf.readInt())
        			.append(",readCounter:").append(indexf.readInt())
        			.append(",writeNum:").append(indexf.readInt())
        			.append(",writePosition:").append(indexf.readInt())
        			.append(",writeCounter:").append(indexf.readInt());
                }
                return sb.toString();
			} catch (Exception e) {
				e.printStackTrace();
			} finally{
				try {
					indexf.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
        }
		return null;
    }
}
