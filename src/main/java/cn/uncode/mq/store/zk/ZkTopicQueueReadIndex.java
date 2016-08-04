package cn.uncode.mq.store.zk;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.cluster.Cluster;
import cn.uncode.mq.store.TopicQueueIndex;
import cn.uncode.mq.util.ZkUtils;
import cn.uncode.mq.zk.ZkClient;

public class ZkTopicQueueReadIndex implements TopicQueueIndex{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ZkTopicQueueReadIndex.class);
	
    private volatile int readNum;        // 8   读索引文件号
    private volatile int readPosition;   // 12   读索引位置
    private volatile int readCounter;    // 16   总读取数量
    private volatile int writeNum;       // 20  写索引文件号
    private volatile int writePosition;  // 24  写索引位置
    private volatile int writeCounter;   // 28 总写入数量
    //ZK
    public static final String ZK_INDEX = ZkUtils.ZK_MQ_BASE + "/index";
    private final ZkClient zkClient;
    private final String queueName;
    private ByteBuffer readIndex;
    private AtomicInteger readTimes = new AtomicInteger(0);
    
    
    public ZkTopicQueueReadIndex(ZkClient zkClient, String queueName) {
    	this.zkClient = zkClient;
    	this.queueName = queueName;
    	ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.ZK_MQ_BASE);
    	if (!zkClient.exists(ZK_INDEX)) {
            zkClient.createPersistent(ZK_INDEX, true);
        }
    	String path = formatZkIndexPath(this.queueName);
		if (zkClient.exists(path)) {
			byte[] datas = zkClient.readData(path);
			if (null != datas) {
				buildFromData(datas);
			} else {
				init();
			}
		} else {
			zkClient.createPersistent(path, true);
			init();
		}
    	
    }

	public void init() {
		readIndex = ByteBuffer.allocate(INDEX_SIZE);
		putMagic();
		putReadNum(0);
		putReadPosition(0);
		putReadCounter(0);
		putWriteNum(0);
		putWritePosition(0);
		putWriteCounter(0);
	}

	public void buildFromData(byte[] datas) {
		readIndex = ByteBuffer.wrap(datas);
		byte[] bytes = new byte[8];
		readIndex.get(bytes, 0, 8);
		if (!MAGIC.equals(new String(bytes))) {
		    throw new IllegalArgumentException("version mismatch");
		}
		this.readNum = readIndex.getInt();
		this.readPosition = readIndex.getInt();
		this.readCounter = readIndex.getInt();
		this.writeNum = readIndex.getInt();
		this.writePosition = readIndex.getInt();
		this.writeCounter = readIndex.getInt();
	}

	private String formatZkIndexPath(String queueName) {
        return ZK_INDEX + "/" + queueName;
    }

    public int getReadNum() {
        return this.readNum;
    }

    public int getReadPosition() {
        return this.readPosition;
    }

    public int getReadCounter() {
        return this.readCounter;
    }
    
    public int getWriteNum() {
        return this.writeNum;
    }

    public int getWritePosition() {
        return this.writePosition;
    }
    
    public int getWriteCounter() {
        return this.writeCounter;
    }

    public void putMagic() {
    	readIndex.position(0);
    	readIndex.put(MAGIC.getBytes());
    }

    public void putReadNum(int readNum) {
    	
    	readIndex.position(READ_NUM_OFFSET);
    	readIndex.putInt(readNum);
        this.readNum = readNum;
    }

    public void putReadPosition(int readPosition) {
    	readIndex.position(READ_POS_OFFSET);
    	readIndex.putInt(readPosition);
        this.readPosition = readPosition;
    }

    public void putReadCounter(int readCounter) {
    	readIndex.position(READ_CNT_OFFSET);
    	readIndex.putInt(readCounter);
        this.readCounter = readCounter;
    }
    
    public void putWritePosition(int writePosition) {
    	readIndex.position(WRITE_POS_OFFSET);
    	readIndex.putInt(writePosition);
        this.writePosition = writePosition;
    }

    public void putWriteNum(int writeNum) {
    	readIndex.position(WRITE_NUM_OFFSET);
    	readIndex.putInt(writeNum);
        this.writeNum = writeNum;
    }
    
    public void putWriteCounter(int writeCounter) {
    	readIndex.position(WRITE_CNT_OFFSET);
    	readIndex.putInt(writeCounter);
        this.writeCounter = writeCounter;
    }
    
    public int activeSyncForRead(){
    	return readTimes.incrementAndGet();
    }

    public void sync() {
    	String path = formatZkIndexPath(this.queueName);
    	if(readTimes.get() > 0){
    		if (readIndex != null) {
            	zkClient.writeData(path, readIndex.array());
            }
    		readTimes.decrementAndGet();
    	}else{
    		byte[] datas = zkClient.readData(path);
    		if(null != datas && datas.length > 0){
    			buildFromData(datas);
    		}else{
    			init();
    		}
    	}
        path += "/" + Cluster.getMaster().getZkIndexMasterSlave();
        if(!zkClient.exists(path)){
        	zkClient.createEphemeral(path, null);
        }
    }

    public void close() {
        sync();
    }

	public String getQueueName() {
		return queueName;
	}

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

	


}
