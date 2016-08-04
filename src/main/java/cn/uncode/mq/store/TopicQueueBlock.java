package cn.uncode.mq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.store.TopicQueueIndex;
import cn.uncode.mq.util.Cleaner;

public class TopicQueueBlock {
	
	private final Logger LOGGER = LoggerFactory.getLogger(TopicQueueBlock.class);
	
	public static final String BLOCK_FILE_SUFFIX = ".umq";//数据文件
	public static final int BLOCK_SIZE = 1 * 1024 * 1024;//32MB
	
	public static final int EOF = -1;
	
	private String blockFilePath;
    private TopicQueueIndex index;
    private RandomAccessFile blockFile;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer;
    private MappedByteBuffer mappedBlock;

    public TopicQueueBlock(String blockFilePath, TopicQueueIndex index, RandomAccessFile blockFile, FileChannel fileChannel,
                        ByteBuffer byteBuffer, MappedByteBuffer mappedBlock) {
        this.blockFilePath = blockFilePath;
        this.index = index;
        this.blockFile = blockFile;
        this.fileChannel = fileChannel;
        this.byteBuffer = byteBuffer;
        this.mappedBlock = mappedBlock;
    }

    public TopicQueueBlock(TopicQueueIndex index, String blockFilePath) {
        this.index = index;
        this.blockFilePath = blockFilePath;
        try {
            File file = new File(blockFilePath);
            this.blockFile = new RandomAccessFile(file, "rw");
            this.fileChannel = blockFile.getChannel();
            this.mappedBlock = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, BLOCK_SIZE);
            this.byteBuffer = mappedBlock.load();
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public TopicQueueBlock duplicate() {
        return new TopicQueueBlock(this.blockFilePath, this.index, this.blockFile, this.fileChannel,
                this.byteBuffer.duplicate(), this.mappedBlock);
    }

    public static String formatBlockFilePath(String queueName, int fileNum, String fileBackupDir) {
        return fileBackupDir + File.separator + String.format("tblock_%s_%d%s", queueName, fileNum, BLOCK_FILE_SUFFIX);
    }

    public String getBlockFilePath() {
        return blockFilePath;
    }

    public void putEOF() {
        this.byteBuffer.position(index.getWritePosition());
        this.byteBuffer.putInt(EOF);
    }

    public boolean isSpaceAvailable(int len) {
        int increment = len + 4;
        int writePosition = index.getWritePosition();
        return BLOCK_SIZE >= increment + writePosition + 4; // 保证最后有4字节的空间可以写入EOF
    }

    public boolean eof() {
        int readPosition = index.getReadPosition();
        return readPosition > 0 && byteBuffer.getInt(readPosition) == EOF;
    }
    
    public int write(byte[] bytes) {
        int len = bytes.length;
        int increment = len + 4;
        int writePosition = index.getWritePosition();
        byteBuffer.position(writePosition);
        byteBuffer.putInt(len);
        byteBuffer.put(bytes);
        index.putWritePosition(increment + writePosition);
        index.putWriteCounter(index.getWriteCounter() + 1);
        return increment;
    }
    
    public byte[] read() {
        byte[] bytes;
        int readNum = index.getReadNum();
        int readPosition = index.getReadPosition();
        int writeNum = index.getWriteNum();
        int writePosition = index.getWritePosition();
        if (readNum == writeNum && readPosition >= writePosition) {
            return null;
        }
        byteBuffer.position(readPosition);
        int dataLength = byteBuffer.getInt();
        if (dataLength <= 0) {
            return null;
        }
        bytes = new byte[dataLength];
        byteBuffer.get(bytes);
        index.putReadPosition(readPosition + bytes.length + 4);
        index.putReadCounter(index.getReadCounter() + 1);
        return bytes;
    }
    
    public boolean eof(int readPosition) {
        return readPosition > 0 && byteBuffer.getInt(readPosition) == EOF;
    }
    
    public byte[] read(int readPosition) {
        byte[] bytes;
        byteBuffer.position(readPosition);
        int dataLength = byteBuffer.getInt();
        if (dataLength <= 0) {
            return null;
        }
        bytes = new byte[dataLength];
        byteBuffer.get(bytes);
        return bytes;
    }

    public void sync() {
        if (mappedBlock != null) {
            mappedBlock.force();
        }
    }

    public void close() {
        try {
            if (mappedBlock == null) {
                return;
            }
            sync();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        Method getCleanerMethod = mappedBlock.getClass().getMethod("cleaner");
                        getCleanerMethod.setAccessible(true);
                        Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(mappedBlock);
                        cleaner.clean();
                    } catch (Exception e) {
                        LOGGER.error("close fqueue block file failed", e);
                    }
                    return null;
                }
            });
            mappedBlock = null;
            byteBuffer = null;
            fileChannel.close();
            blockFile.close();
        } catch (IOException e) {
            LOGGER.error("close fqueue block file failed", e);
        }
    }
    
    

}
