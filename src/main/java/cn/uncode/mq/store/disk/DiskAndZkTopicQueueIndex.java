package cn.uncode.mq.store.disk;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.store.TopicQueueIndex;
import cn.uncode.mq.store.zk.ZkTopicQueueReadIndex;
import cn.uncode.mq.util.Cleaner;
import cn.uncode.mq.zk.ZkClient;

public class DiskAndZkTopicQueueIndex implements TopicQueueIndex {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DiskAndZkTopicQueueIndex.class);
	
	private static final String INDEX_FILE_SUFFIX = ".umq";
    private volatile int readNum;        // 8   读索引文件号
    private volatile int readPosition;   // 12   读索引位置
    private volatile int readCounter;    // 16   总读取数量
    private volatile int writeNum;       // 20  写索引文件号
    private volatile int writePosition;  // 24  写索引位置
    private volatile int writeCounter;   // 28 总写入数量

    private RandomAccessFile indexFile;
    private FileChannel fileChannel;
    // 读写分离
    private MappedByteBuffer writeIndex;
    private MappedByteBuffer readIndex;
    
//    private final boolean backup;
    //zk
    private ZkTopicQueueReadIndex zkReadIndex;
    
    
//    public DiskAndZkTopicQueueIndex(String queueName, String fileDir, ZkClient zkClient) {
//    	this(queueName, fileDir, zkClient, false);
//    }

    public DiskAndZkTopicQueueIndex(String queueName, String fileDir, ZkClient zkClient) {
//    	this.backup = backup;
    	String indexFilePath = DiskAndZkTopicQueueIndex.formatIndexFilePath(queueName, fileDir);
        File file = new File(indexFilePath);
        try {
            if (file.exists()) {
                this.indexFile = new RandomAccessFile(file, "rw");
                byte[] bytes = new byte[8];
                this.indexFile.read(bytes, 0, 8);
                if (!MAGIC.equals(new String(bytes))) {
                    throw new IllegalArgumentException("version mismatch");
                }
                this.readNum = indexFile.readInt();
                this.readPosition = indexFile.readInt();
                this.readCounter = indexFile.readInt();
                this.writeNum = indexFile.readInt();
                this.writePosition = indexFile.readInt();
                this.writeCounter = indexFile.readInt();
                this.fileChannel = indexFile.getChannel();
                this.writeIndex = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);
                this.writeIndex = writeIndex.load();
                this.readIndex = (MappedByteBuffer) writeIndex.duplicate();
            } else {
                this.indexFile = new RandomAccessFile(file, "rw");
                this.fileChannel = indexFile.getChannel();
                this.writeIndex = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);
                this.readIndex = (MappedByteBuffer) writeIndex.duplicate();
                putMagic();
                putReadNum(0);
                putReadPosition(0);
                putReadCounter(0);
                putWriteNum(0);
                putWritePosition(0);
                putWriteCounter(0);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        //zk
        if(null != zkClient){
        	this.zkReadIndex = new ZkTopicQueueReadIndex(zkClient, queueName);
        }
        
    }

    public static boolean isIndexFile(String fileName) {
        return fileName.endsWith(INDEX_FILE_SUFFIX);
    }

    public static String parseQueueName(String indexFileName) {
        String fileName = indexFileName.substring(0, indexFileName.lastIndexOf('.'));
        return fileName.split("_")[1];
    }

    public static String formatIndexFilePath(String queueName, String fileBackupDir) {
        return fileBackupDir + File.separator + String.format("tindex_%s%s", queueName, INDEX_FILE_SUFFIX);
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#getReadNum()
	 */
    @Override
	public int getReadNum() {
        return this.readNum;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#getReadPosition()
	 */
    @Override
	public int getReadPosition() {
        return this.readPosition;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#getReadCounter()
	 */
    @Override
	public int getReadCounter() {
        return this.readCounter;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#getWriteNum()
	 */
    @Override
	public int getWriteNum() {
        return this.writeNum;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#getWritePosition()
	 */
    @Override
	public int getWritePosition() {
        return this.writePosition;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#getWriteCounter()
	 */
    @Override
	public int getWriteCounter() {
        return this.writeCounter;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putMagic()
	 */
    @Override
	public void putMagic() {
        this.writeIndex.position(0);
        this.writeIndex.put(MAGIC.getBytes());
        if(zkReadIndex != null){
        	zkReadIndex.putMagic();
        }
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putWritePosition(int)
	 */
    @Override
	public void putWritePosition(int writePosition) {
        this.writeIndex.position(WRITE_POS_OFFSET);
        this.writeIndex.putInt(writePosition);
        if(zkReadIndex != null){
        	zkReadIndex.putWritePosition(writePosition);
        }
        this.writePosition = writePosition;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putWriteNum(int)
	 */
    @Override
	public void putWriteNum(int writeNum) {
        this.writeIndex.position(WRITE_NUM_OFFSET);
        this.writeIndex.putInt(writeNum);
        if(zkReadIndex != null){
        	zkReadIndex.putWriteNum(writeNum);
        }
        this.writeNum = writeNum;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putWriteCounter(int)
	 */
    @Override
	public void putWriteCounter(int writeCounter) {
        this.writeIndex.position(WRITE_CNT_OFFSET);
        this.writeIndex.putInt(writeCounter);
        if(zkReadIndex != null){
        	zkReadIndex.putWriteCounter(writeCounter);
        }
        this.writeCounter = writeCounter;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putReadNum(int)
	 */
    @Override
	public void putReadNum(int readNum) {
        this.readIndex.position(READ_NUM_OFFSET);
        this.readIndex.putInt(readNum);
        if(zkReadIndex != null){
        	zkReadIndex.putReadNum(readNum);
        }
        this.readNum = readNum;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putReadPosition(int)
	 */
    @Override
	public void putReadPosition(int readPosition) {
        this.readIndex.position(READ_POS_OFFSET);
        this.readIndex.putInt(readPosition);
        if(zkReadIndex != null){
        	zkReadIndex.putReadPosition(readPosition);
        }
        this.readPosition = readPosition;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putReadCounter(int)
	 */
    @Override
	public void putReadCounter(int readCounter) {
        this.readIndex.position(READ_CNT_OFFSET);
        this.readIndex.putInt(readCounter);
        if(zkReadIndex != null){
        	zkReadIndex.putReadCounter(readCounter);
        }
        this.readCounter = readCounter;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#reset()
	 */
    @Override
	public void reset() {
        int size = writeCounter - readCounter;
        putReadCounter(0);
        if(zkReadIndex != null){
        	zkReadIndex.putReadCounter(0);
        }
        putWriteCounter(size);
        if (size == 0 && readNum == writeNum) {
            putReadPosition(0);
            putWritePosition(0);
            if(zkReadIndex != null){
            	zkReadIndex.putReadPosition(0);
            }
        }
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#sync()
	 */
    @Override
	public void sync() {
        if (writeIndex != null) {
            writeIndex.force();
        }
        if (null != this.zkReadIndex) {
        	zkReadIndex.sync();
        }
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#close()
	 */
    @Override
	public void close() {
        try {
            if (writeIndex == null) {
                return;
            }
            sync();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        Method getCleanerMethod = writeIndex.getClass().getMethod("cleaner");
                        getCleanerMethod.setAccessible(true);
                        Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(writeIndex);
                        cleaner.clean();
                    } catch (Exception e) {
                        LOGGER.error("close fqueue index file failed", e);
                    }
                    return null;
                }
            });
            writeIndex = null;
            readIndex = null;
            fileChannel.close();
            indexFile.close();
            zkReadIndex.close();
        } catch (IOException e) {
            LOGGER.error("close fqueue index file failed", e);
        }
    }

}
