package cn.uncode.mq.store;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.server.backup.BackupQueuePool;
import cn.uncode.mq.zk.ZkClient;

public class TopicQueuePool {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicQueuePool.class);
	
	public static final String INDEX_FILE_SUFFIX = ".umq";
	
	private static final String DEFAULT_DATA_PATH = "./data";
	public static final String DATA_BACKUP_PATH = "/backup";
	
    private static final BlockingQueue<String> DELETING_QUEUE = new LinkedBlockingQueue<>();
    private static TopicQueuePool INSTANCE = null;
    private final String filePath;
    private Map<String, TopicQueue> queueMap;
//    private Map<String, TopicQueue> backupQueueMap;
    private ScheduledExecutorService syncService;
    private final ZkClient zkClient;

    private TopicQueuePool(ZkClient zkClient, String filePath) {
    	this.zkClient = zkClient;
    	if(StringUtils.isBlank(filePath)){
    		this.filePath = DEFAULT_DATA_PATH;
    	}else{
    		this.filePath = filePath;
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
//        this.backupQueueMap = scanDir(fileBackupDir, true);
        this.syncService = Executors.newSingleThreadScheduledExecutor();
        this.syncService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
            	//master
                for (TopicQueue queue : queueMap.values()) {
                	queue.sync();
                }
                //deleteBlockFile(); 暂不作删除操作
                //backup
//                for (TopicQueue queue : backupQueueMap.values()) {
//                	queue.sync();
//                }
                
            }
        }, 10L, 10L, TimeUnit.SECONDS);
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
    
    public synchronized static void startup(String dataPath) {
        startup(null, dataPath);
    }

    public synchronized static void startup(ZkClient zkClient, String dataPath) {
        if (INSTANCE == null) {
            INSTANCE = new TopicQueuePool(zkClient, dataPath);
        }
        String path = null;
        if(StringUtils.isBlank(dataPath)){
        	path = DEFAULT_DATA_PATH + DATA_BACKUP_PATH;
    	}else{
    		path = dataPath + DATA_BACKUP_PATH;
    	}
        BackupQueuePool.startup(zkClient, path);
    }

    private void disposal() {
        this.syncService.shutdown();
        //master
        for (TopicQueue queue : queueMap.values()) {
        	queue.close();
        }
        //backup
//        for (TopicQueue queue : backupQueueMap.values()) {
//        	queue.close();
//        }
        //暂不执行
        /*while (!DELETING_QUEUE.isEmpty()) {
            deleteBlockFile();
        }*/
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
//        if(INSTANCE.backupQueueMap.containsKey(queueName)){
//        	return INSTANCE.backupQueueMap.get(queueName);
//        }
        return null;
    }

    public synchronized static TopicQueue getQueueOrCreate(String queueName) {
    	TopicQueue topicQueue = null;
        if (StringUtils.isBlank(queueName)) {
            throw new IllegalArgumentException("empty queue name");
        }
        String fileDir = INSTANCE.filePath;
//        if(!backup){
        topicQueue = INSTANCE.getQueueFromPool(INSTANCE.queueMap, queueName, fileDir);
//        }else{
//        	fileDir += DATA_BACKUP_PATH;
//        	topicQueue = INSTANCE.getQueueFromPool(INSTANCE.backupQueueMap, queueName, fileDir, backup);
//        }
        return topicQueue;
    }
    
	
    public static boolean isIndexFile(String fileName) {
        return fileName.endsWith(INDEX_FILE_SUFFIX);
    }
    
    public static String parseQueueName(String indexFileName) {
        String fileName = indexFileName.substring(0, indexFileName.lastIndexOf('.'));
        return fileName.split("_")[1];
    }

}
