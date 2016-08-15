package cn.uncode.mq.store;

import java.io.File;
import java.io.FilenameFilter;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.server.backup.BackupQueuePool;
import cn.uncode.mq.store.disk.DiskAndZkTopicQueueIndex;
import cn.uncode.mq.util.Scheduler;
import cn.uncode.mq.zk.ZkClient;

public class TopicQueuePool {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicQueuePool.class);
	
	public static final String INDEX_FILE_SUFFIX = ".umq";
	
	public static final String DEFAULT_DATA_PATH = "./data";
	public static final String DATA_BACKUP_PATH = "/backup";
	
	public static int ZK_SYNC_DATA_PERSISTENCE_INTERVAL = 3;
	
    private static final BlockingQueue<String> DELETING_QUEUE = new LinkedBlockingQueue<>();
    private static TopicQueuePool INSTANCE = null;
    private final String filePath;
    private Map<String, TopicQueue> queueMap;
    private final Scheduler scheduler = new Scheduler(1, "uncode-mq-topic-queue-", false);
    private final ZkClient zkClient;

    private TopicQueuePool(ZkClient zkClient, ServerConfig config) {
    	this.zkClient = zkClient;
    	if(StringUtils.isBlank(config.getDataDir())){
    		this.filePath = DEFAULT_DATA_PATH;
    	}else{
    		this.filePath = config.getDataDir();
    	}
        File fileDir = new File(this.filePath);
		if (!fileDir.exists()) {
			fileDir.mkdirs();
        }
        if (!fileDir.isDirectory() || !fileDir.canRead()) {
            throw new IllegalArgumentException(fileDir.getAbsolutePath() + " is not a readable log directory.");
        }
        File fileBackupDir = new File(this.filePath + DATA_BACKUP_PATH);
		if (!fileBackupDir.exists()) {
			fileBackupDir.mkdirs();
        }
		if (!fileBackupDir.isDirectory() || !fileBackupDir.canRead()) {
            throw new IllegalArgumentException(fileBackupDir.getAbsolutePath() + " is not a readable log directory.");
        }
        this.queueMap = scanDir(fileDir, false);
        long delay = config.getDataPersistenceInterval();
        ZK_SYNC_DATA_PERSISTENCE_INTERVAL = (int) (config.getZKDataPersistenceInterval()/delay);
        scheduler.scheduleWithRate(new Runnable() {
            @Override
            public void run() {
                for (TopicQueue queue : queueMap.values()) {
                	queue.sync();
                }
                deleteBlockFile();
            }
        }, 1000L, delay * 1000L);
    }

    private void deleteBlockFile() {
        String blockFilePath = DELETING_QUEUE.poll();
        if (StringUtils.isNotBlank(blockFilePath)) {
            File delFile = new File(blockFilePath);
            try {
                if (!delFile.delete()) {
                    LOGGER.warn("block file:{} delete failed", blockFilePath);
                }
            } catch (SecurityException e) {
                LOGGER.error("security manager exists, delete denied");
            }
        }
    }

    public static void toClear(String filePath) {
        DELETING_QUEUE.add(filePath);
    }

    /**
     * 
     * @param pathDir
     * @param buckup 是否为备份节点
     * @return
     */
    private Map<String, TopicQueue> scanDir(File pathDir, boolean backup) {
        if (!pathDir.isDirectory()) {
            throw new IllegalArgumentException("it is not a directory");
        }
        Map<String, TopicQueue> exitsFQueues = new HashMap<>();
        File[] indexFiles = pathDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return isIndexFile(name);
            }
        });
        if (ArrayUtils.isNotEmpty(indexFiles)) {
            for (File indexFile : indexFiles) {
                String queueName = parseQueueName(indexFile.getName());
                exitsFQueues.put(queueName, new TopicQueue(queueName, pathDir.getAbsolutePath(), this.zkClient, backup));
            }
        }
        return exitsFQueues;
    }
    
    public synchronized static void startup(ServerConfig config) {
        startup(null, config);
    }

    public synchronized static void startup(ZkClient zkClient, ServerConfig config) {
        if (INSTANCE == null) {
            INSTANCE = new TopicQueuePool(zkClient, config);
        }
        BackupQueuePool.startup(zkClient, config);
    }

    private void disposal() {
        this.scheduler.shutdown();
        for (TopicQueue queue : queueMap.values()) {
        	queue.close();
        }
        while (!DELETING_QUEUE.isEmpty()) {
            deleteBlockFile();
        }
    }

    public synchronized static void destory() {
        if (INSTANCE != null) {
            INSTANCE.disposal();
            INSTANCE = null;
        }
    }

    private TopicQueue getQueueFromPool(Map<String, TopicQueue> queueMap, String queueName, String filePath) {
        if (queueMap.containsKey(queueName)) {
            return queueMap.get(queueName);
        }
        TopicQueue queue = new TopicQueue(queueName, filePath, zkClient, false);
        queueMap.put(queueName, queue);
        return queue;
    }
    
    public synchronized static TopicQueue getQueue(String queueName) {
        if (StringUtils.isBlank(queueName)) {
            throw new IllegalArgumentException("empty queue name");
        }
        if (INSTANCE.queueMap.containsKey(queueName)) {
            return INSTANCE.queueMap.get(queueName);
        }
        return null;
    }

    public synchronized static TopicQueue getQueueOrCreate(String queueName) {
    	TopicQueue topicQueue = null;
        if (StringUtils.isBlank(queueName)) {
            throw new IllegalArgumentException("empty queue name");
        }
        String fileDir = INSTANCE.filePath;
        topicQueue = INSTANCE.getQueueFromPool(INSTANCE.queueMap, queueName, fileDir);
        return topicQueue;
    }
    
	
    public static boolean isIndexFile(String fileName) {
        return fileName.endsWith(INDEX_FILE_SUFFIX);
    }
    
    public static String parseQueueName(String indexFileName) {
        String fileName = indexFileName.substring(0, indexFileName.lastIndexOf('.'));
        return fileName.split("_")[1];
    }
    
    public static Set<String> getQueueNameFromDisk(){
    	Set<String> names = null;
		File fileDir = new File(INSTANCE.filePath);
		if (fileDir.exists() && fileDir.isDirectory() && fileDir.canRead()) {
			File[] indexFiles = fileDir.listFiles(new FilenameFilter() {
	            @Override
	            public boolean accept(File dir, String name) {
	            	return name.startsWith("tindex") && name.endsWith(TopicQueuePool.INDEX_FILE_SUFFIX);
	            }
	        });
	        if (ArrayUtils.isNotEmpty(indexFiles)) {
	        	names = new HashSet<String>();
	            for (File indexFile : indexFiles) {
	            	String[] nas = indexFile.getName().split("_");
	            	if(nas != null && nas.length > 1){
	            		names.add(nas[1].replace(INDEX_FILE_SUFFIX, ""));
	            	}
	            }
	        }
		}
    	return names;
    }
    
    
    public static void main(String[] args){
		try {
			String indexFilePath = DiskAndZkTopicQueueIndex.formatIndexFilePath("test", DEFAULT_DATA_PATH);
	        File file = new File(indexFilePath);
	        String ms = "master:";
            if (file.exists()) {
            	RandomAccessFile indexf = new RandomAccessFile(file, "rw");
                byte[] bytes = new byte[8];
                indexf.read(bytes, 0, 8);
                String MAGIC = "umqv.1.0";
                StringBuilder sb  = new StringBuilder(ms);
                if (!MAGIC.equals(new String(bytes))) {
                    
                }else{
                	sb.append(MAGIC).append("=>").append("readNum:").append(indexf.readInt())
        			.append(",readPosition:").append(indexf.readInt())
        			.append(",readCounter:").append(indexf.readInt())
        			.append(",writeNum:").append(indexf.readInt())
        			.append(",writePosition:").append(indexf.readInt())
        			.append(",writeCounter:").append(indexf.readInt());
                }
                System.out.println(sb.toString());
            }else{
            	System.out.println(ms);
            }
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
    }

}

