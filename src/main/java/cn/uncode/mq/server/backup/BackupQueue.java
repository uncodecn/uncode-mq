package cn.uncode.mq.server.backup;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.network.Message;
import cn.uncode.mq.store.TopicQueueBlock;
import cn.uncode.mq.store.TopicQueueIndex;
import cn.uncode.mq.store.disk.DiskTopicQueueIndex;
import cn.uncode.mq.store.zk.ZkTopicQueueReadIndex;
import cn.uncode.mq.zk.ZkClient;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

public class BackupQueue extends AbstractQueue<byte[]> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(BackupQueue.class);

    private String queueName;
    private String fileDir;
    private DiskTopicQueueIndex writeIndex;
    private ZkTopicQueueReadIndex readIndex;
    private BackupQueueBlock readBlock;
    private BackupQueueBlock writeBlock;
    private ReentrantLock readLock;
    private ReentrantLock writeLock;
    private AtomicInteger size;
    
    public BackupQueue(String queueName, String fileDir, ZkClient zkClient) {
    	this.queueName = queueName;
        this.fileDir = fileDir;
        this.readLock = new ReentrantLock();
        this.writeLock = new ReentrantLock();
    	this.readIndex = new ZkTopicQueueReadIndex(zkClient, queueName);
    	this.writeIndex = new DiskTopicQueueIndex(queueName, fileDir);
        this.size = new AtomicInteger(writeIndex.getWriteCounter() - readIndex.getReadCounter());
        String filePath = BackupQueueBlock.formatBlockFilePath(queueName, writeIndex.getWriteNum(), fileDir);
        this.writeBlock = new BackupQueueBlock(writeIndex, readIndex, filePath);
        if (readIndex.getReadNum() == writeIndex.getWriteNum()) {
            this.readBlock = this.writeBlock.duplicate();
        } else {
        	filePath = BackupQueueBlock.formatBlockFilePath(queueName, readIndex.getReadNum(), fileDir);
            this.readBlock = new BackupQueueBlock(writeIndex, readIndex, filePath);
        }
    }
    
    public TopicQueueIndex getReadIndex(){
    	return readIndex;
    }

    @Override
    public Iterator<byte[]> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return this.size.get();
    }

    private void rotateNextWriteBlock() {
        int nextWriteBlockNum = writeIndex.getWriteNum() + 1;
        nextWriteBlockNum = (nextWriteBlockNum < 0) ? 0 : nextWriteBlockNum;
        writeBlock.putEOF();
        if (readIndex.getReadNum() == writeIndex.getWriteNum()) {
            writeBlock.sync();
        } else {
            writeBlock.close();
        }
        writeBlock = new BackupQueueBlock(writeIndex, readIndex, TopicQueueBlock.formatBlockFilePath(queueName,
                nextWriteBlockNum, fileDir));
        writeIndex.putWriteNum(nextWriteBlockNum);
        writeIndex.putWritePosition(0);
    }
    
    public boolean write(Message... messages) {
    	PooledByteBufAllocator pooledByteBufAllocator = new PooledByteBufAllocator();
    	ByteBuf byteBuf = pooledByteBufAllocator.directBuffer();
    	if(null != messages){
        	for (Message message : messages) {
        		message.writeToByteBuf(byteBuf);
            }
        }
		if(byteBuf.hasArray()){
			return offer(byteBuf.array());
		}
		return false;
    }
    

    @Override
    public boolean offer(byte[] bytes) {
        if (ArrayUtils.isEmpty(bytes)) {
            return true;
        }
        writeLock.lock();
        try {
            if (!writeBlock.isSpaceAvailable(bytes.length)) {
                rotateNextWriteBlock();
            }
            writeBlock.write(bytes);
            size.incrementAndGet();
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    
    
    @Override
    public byte[] poll() {
    	readLock.lock();
        try {
            if (readBlock.eof()) {
                rotateNextReadBlock();
            }
            byte[] bytes = readBlock.read();
            if (bytes != null) {
                size.decrementAndGet();
            }
            return bytes;
        } finally {
            readLock.unlock();
        }
    }
    
    private void rotateNextReadBlock() {
        if (readIndex.getReadNum() == writeIndex.getWriteNum()) {
            // 读缓存块的滑动必须发生在写缓存块滑动之后
            return;
        }
        int nextReadBlockNum = readIndex.getReadNum() + 1;
        nextReadBlockNum = (nextReadBlockNum < 0) ? 0 : nextReadBlockNum;
        readBlock.close();
        String blockPath = readBlock.getBlockFilePath();
        if (nextReadBlockNum == writeIndex.getWriteNum()) {
            readBlock = writeBlock.duplicate();
        } else {
            readBlock = new BackupQueueBlock(writeIndex, readIndex, BackupQueueBlock.formatBlockFilePath(queueName,
                    nextReadBlockNum, fileDir));
        }
        readIndex.putReadNum(nextReadBlockNum);
        readIndex.putReadPosition(0);
        BackupQueuePool.toClear(blockPath);
    }

    @Override
    public byte[] peek() {
        throw new UnsupportedOperationException();
    }

    public void sync() {
    	try {
    		readIndex.sync();
		} catch (Exception e) {
			LOGGER.error("sync to zk error", e);
		}
    	writeIndex.sync();
        // read block只读，不用同步
        writeBlock.sync();
    }

    public DiskTopicQueueIndex getWriteIndex() {
		return writeIndex;
	}

	public void close() {
        writeBlock.close();
        writeIndex.reset();
        writeIndex.close();
        readIndex.close();
    }


}
