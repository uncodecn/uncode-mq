package cn.uncode.mq.server.backup;

import static cn.uncode.mq.store.TopicQueuePool.DATA_BACKUP_PATH;
import static cn.uncode.mq.store.TopicQueuePool.DEFAULT_DATA_PATH;
import static cn.uncode.mq.store.TopicQueuePool.INDEX_FILE_SUFFIX;

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
import cn.uncode.mq.store.TopicQueuePool;
import cn.uncode.mq.store.disk.DiskAndZkTopicQueueIndex;
import cn.uncode.mq.util.Scheduler;
import cn.uncode.mq.zk.ZkClient;

public class BackupQueuePool {
	
	
	private static final Logger LOGGER = LoggerFactory.getLogger(TopicQueuePool.class);
	
	private static final BlockingQueue<String> DELETING_QUEUE = new LinkedBlockingQueue<>();
    private static BackupQueuePool INSTANCE = null;
    private String filePath;
    private Map<String, BackupQueue> backupQueueMap;
    private final Scheduler scheduler = new Scheduler(1, "uncode-mq-backup-queue-", false);
    private final ZkClient zkClient;

    private BackupQueuePool(ZkClient zkClient, ServerConfig config) {
    	this.zkClient = zkClient;
    	this.filePath = DEFAULT_DATA_PATH + DATA_BACKUP_PATH;
        if(StringUtils.isNotBlank(config.getDataDir())){
        	this.filePath = config.getDataDir() + DATA_BACKUP_PATH;
    	}
        File fileDir = new File(this.filePath);
		if (!fileDir.exists()) {
			fileDir.mkdirs();
        }
        if (!fileDir.isDirectory() || !fileDir.canRead()) {
            throw new IllegalArgumentException(fileDir.getAbsolutePath() + " is not a readable log directory.");
        }
        this.backupQueueMap = scanDir(fileDir);
        this.scheduler.scheduleWithRate(new Runnable() {
            @Override
            public void run() {
                for (BackupQueue queue : backupQueueMap.values()) {
                	queue.sync();
                }
                //deleteBlockFile(); 暂不作删除操作
            }
        }, 1000L, 4000L);
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
    
    public synchronized static void startup(ServerConfig config) {
        startup(null, config);
    }

    public synchronized static void startup(ZkClient zkClient, ServerConfig config) {
        if (INSTANCE == null) {
            INSTANCE = new BackupQueuePool(zkClient, config);
        }
    }

    private void disposal() {
        this.scheduler.shutdown();
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
    
    public static Set<String> getBackupQueueNameFromDisk(){
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
			String indexFilePath = DiskAndZkTopicQueueIndex.formatIndexFilePath("test", DEFAULT_DATA_PATH+DATA_BACKUP_PATH);
	        File file = new File(indexFilePath);
	        String ms = "slave:";
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

