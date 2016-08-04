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
import cn.uncode.mq.util.Cleaner;

public class DiskTopicQueueIndex implements TopicQueueIndex {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DiskTopicQueueIndex.class);
	
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
    

    public DiskTopicQueueIndex(String queueName, String fileDir) {
    	String indexFilePath = DiskTopicQueueIndex.formatIndexFilePath(queueName, fileDir);
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
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putWritePosition(int)
	 */
    @Override
	public void putWritePosition(int writePosition) {
        this.writeIndex.position(WRITE_POS_OFFSET);
        this.writeIndex.putInt(writePosition);
        this.writePosition = writePosition;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putWriteNum(int)
	 */
    @Override
	public void putWriteNum(int writeNum) {
        this.writeIndex.position(WRITE_NUM_OFFSET);
        this.writeIndex.putInt(writeNum);
        this.writeNum = writeNum;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putWriteCounter(int)
	 */
    @Override
	public void putWriteCounter(int writeCounter) {
        this.writeIndex.position(WRITE_CNT_OFFSET);
        this.writeIndex.putInt(writeCounter);
        this.writeCounter = writeCounter;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putReadNum(int)
	 */
    @Override
	public void putReadNum(int readNum) {
        this.readIndex.position(READ_NUM_OFFSET);
        this.readIndex.putInt(readNum);
        this.readNum = readNum;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putReadPosition(int)
	 */
    @Override
	public void putReadPosition(int readPosition) {
        this.readIndex.position(READ_POS_OFFSET);
        this.readIndex.putInt(readPosition);
        this.readPosition = readPosition;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#putReadCounter(int)
	 */
    @Override
	public void putReadCounter(int readCounter) {
        this.readIndex.position(READ_CNT_OFFSET);
        this.readIndex.putInt(readCounter);
        this.readCounter = readCounter;
    }

    /* (non-Javadoc)
	 * @see cn.uncode.mq.store.disk.QueueIndex#reset()
	 */
    @Override
	public void reset() {
        int size = writeCounter - readCounter;
        putReadCounter(0);
        putWriteCounter(size);
        if (size == 0 && readNum == writeNum) {
            putReadPosition(0);
            putWritePosition(0);
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
        } catch (IOException e) {
            LOGGER.error("close fqueue index file failed", e);
        }
    }

}
