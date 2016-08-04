package cn.uncode.mq.server.backup;

import static cn.uncode.mq.store.TopicQueuePool.INDEX_FILE_SUFFIX;

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

import cn.uncode.mq.store.TopicQueuePool;
import cn.uncode.mq.zk.ZkClient;

public class BackupQueuePool {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicQueuePool.class);
	
	private static final BlockingQueue<String> DELETING_QUEUE = new LinkedBlockingQueue<>();
    private static BackupQueuePool INSTANCE = null;
    private final String filePath;
    private Map<String, BackupQueue> backupQueueMap;
    private ScheduledExecutorService syncService;
    private final ZkClient zkClient;

    private BackupQueuePool(ZkClient zkClient, String filePath) {
    	this.zkClient = zkClient;
    	this.filePath = filePath;
        File fileDir = new File(this.filePath);
		if (!fileDir.exists()) {
			fileDir.mkdirs();
        }
        if (!fileDir.isDirectory() || !fileDir.canRead()) {
            throw new IllegalArgumentException(fileDir.getAbsolutePath() + " is not a readable log directory.");
        }
        this.backupQueueMap = scanDir(fileDir);
        this.syncService = Executors.newSingleThreadScheduledExecutor();
        this.syncService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                for (BackupQueue queue : backupQueueMap.values()) {
                	queue.sync();
                }
                //deleteBlockFile(); 暂不作删除操作
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
     * @return
     */
    private Map<String, BackupQueue> scanDir(File pathDir) {
        if (!pathDir.isDirectory()) {
            throw new IllegalArgumentException("it is not a directory");
        }
        Map<String, BackupQueue> exitsFQueues = new HashMap<>();
        File[] indexFiles = pathDir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return isIndexFile(name);
            }
        });
        if (ArrayUtils.isNotEmpty(indexFiles)) {
            for (File indexFile : indexFiles) {
                String queueName = parseQueueName(indexFile.getName());
                exitsFQueues.put(queueName, new BackupQueue(queueName, pathDir.getAbsolutePath(), this.zkClient));
            }
        }
        return exitsFQueues;
    }
    
    public synchronized static void startup(String dataPath) {
        startup(null, dataPath);
    }

    public synchronized static void startup(ZkClient zkClient, String dataPath) {
        if (INSTANCE == null) {
            INSTANCE = new BackupQueuePool(zkClient, dataPath);
        }
    }

    private void disposal() {
        this.syncService.shutdown();
        for (BackupQueue queue : backupQueueMap.values()) {
        	queue.close();
        }
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

    public synchronized static BackupQueue getBackupQueueFromPool(String queueName) {
        if (INSTANCE.backupQueueMap.containsKey(queueName)) {
            return INSTANCE.backupQueueMap.get(queueName);
        }
        BackupQueue queue = new BackupQueue(queueName, INSTANCE.filePath, INSTANCE.zkClient);
        INSTANCE.backupQueueMap.put(queueName, queue);
        return queue;
    }
    
    public static boolean isIndexFile(String fileName) {
        return fileName.endsWith(INDEX_FILE_SUFFIX);
    }
    
    public static String parseQueueName(String indexFileName) {
        String fileName = indexFileName.substring(0, indexFileName.lastIndexOf('.'));
        return fileName.split("_")[1];
    }
	
		
		
	
}
