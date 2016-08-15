package cn.uncode.mq.store.zk;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
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
    private ByteBuffer index;
    private AtomicInteger readTimes = new AtomicInteger(0);
    
    
    public ZkTopicQueueReadIndex(ZkClient zkClient, String queueName) {
    	this.zkClient = zkClient;
    	this.queueName = queueName;
    	ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.ZK_MQ_BASE);
    	if (!zkClient.exists(ZK_INDEX)) {
            zkClient.createPersistent(ZK_INDEX, true);
        }
    	String path = formatZkIndexPath(this.queueName);
    	path += "/" + Cluster.getCurrent().getMaster().getHost();
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
		index = ByteBuffer.allocate(INDEX_SIZE);
		putMagic();
		putReadNum(0);
		putReadPosition(0);
		putReadCounter(0);
		putWriteNum(0);
		putWritePosition(0);
		putWriteCounter(0);
	}

	public void buildFromData(byte[] datas) {
		index = ByteBuffer.wrap(datas);
		byte[] bytes = new byte[8];
		index.get(bytes, 0, 8);
		if (!MAGIC.equals(new String(bytes))) {
		    throw new IllegalArgumentException("version mismatch");
		}
		this.readNum = index.getInt();
		this.readPosition = index.getInt();
		this.readCounter = index.getInt();
		this.writeNum = index.getInt();
		this.writePosition = index.getInt();
		this.writeCounter = index.getInt();
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
    	index.position(0);
    	index.put(MAGIC.getBytes());
    }

    public void putReadNum(int readNum) {
    	
    	index.position(READ_NUM_OFFSET);
    	index.putInt(readNum);
        this.readNum = readNum;
    }

    public void putReadPosition(int readPosition) {
    	index.position(READ_POS_OFFSET);
    	index.putInt(readPosition);
        this.readPosition = readPosition;
    }

    public void putReadCounter(int readCounter) {
    	index.position(READ_CNT_OFFSET);
    	index.putInt(readCounter);
        this.readCounter = readCounter;
    }
    
    public void putWritePosition(int writePosition) {
    	index.position(WRITE_POS_OFFSET);
    	index.putInt(writePosition);
        this.writePosition = writePosition;
    }

    public void putWriteNum(int writeNum) {
    	index.position(WRITE_NUM_OFFSET);
    	index.putInt(writeNum);
        this.writeNum = writeNum;
    }
    
    public void putWriteCounter(int writeCounter) {
    	index.position(WRITE_CNT_OFFSET);
    	index.putInt(writeCounter);
        this.writeCounter = writeCounter;
    }
    
    public int activeSyncForRead(){
    	return readTimes.incrementAndGet();
    }

    public void sync() {
    	String path = formatZkIndexPath(this.queueName);
    	String slaveOf = null;
    	if(Cluster.getCurrent() != null){
    		slaveOf = Cluster.getCurrent().getSlaveOf().getHost();
    	}
    	if(StringUtils.isNotBlank(slaveOf)){
    		if(readTimes.get() > 0){
        		if (index != null) {
        			ZkUtils.makeSurePersistentPathExists(zkClient, path + "/" + slaveOf);
                	zkClient.writeData(path + "/" + slaveOf, index.array());
                	index.position(0);
                    StringBuilder sb  = new StringBuilder();
                    byte[] bytes = new byte[8];
                    index.get(bytes, 0, 8);
                    sb.append("zk index").append("=>").append("readNum:").append(index.getInt())
        			.append(",readPosition:").append(index.getInt())
        			.append(",readCounter:").append(index.getInt())
        			.append(",writeNum:").append(index.getInt())
        			.append(",writePosition:").append(index.getInt())
        			.append(",writeCounter:").append(index.getInt());
                    //LOGGER.info(sb.toString());
                }
        		readTimes.decrementAndGet();
        	}else{
        		if(zkClient.exists(path + "/" + slaveOf)){
        			byte[] datas = zkClient.readData(path + "/" + slaveOf);
            		if(null != datas && datas.length > 0){
            			buildFromData(datas);
            		}else{
            			init();
            		}
        		}else{
        			init();
        		}
        		StringBuilder sb  = new StringBuilder();
                sb.append("index read from zk").append("=>").append("readNum:").append(getReadNum())
    			.append(",readPosition:").append(getReadPosition())
    			.append(",readCounter:").append(getReadCounter())
    			.append(",writeNum:").append(getWriteNum())
    			.append(",writePosition:").append(getWritePosition())
    			.append(",writeCounter:").append(getWriteCounter());
                //LOGGER.info(sb.toString());
        	}
		}
    	if(Cluster.getCurrent() != null){
    		path += "/" + Cluster.getCurrent().getZkIndexMasterSlave();
            if(!zkClient.exists(path)){
            	zkClient.createEphemeral(path, null);
            }
    	}
    }
    
    public void sync2zk(int readNum, int readPosition, int readCounter, int writeNum, int writePosition, int writeCounter) {
    	ZkUtils.getCluster(zkClient);
    	String path = formatZkIndexPath(this.queueName);
    	if(Cluster.getCurrent() != null){
    		if(Cluster.getCurrent().getMaster().getHost().equals(Cluster.getMasterIps().peek())){
            	index = ByteBuffer.allocate(INDEX_SIZE);
        		putMagic();
        		putReadNum(readNum);
        		putReadPosition(readPosition);
        		putReadCounter(readCounter);
        		putWriteNum(writeNum);
        		putWritePosition(writePosition);
        		putWriteCounter(writeCounter);
        		ZkUtils.makeSurePersistentPathExists(zkClient, path + "/" + Cluster.getCurrent().getMaster().getHost());
        		zkClient.writeData(path + "/" + Cluster.getCurrent().getMaster().getHost(), index.array());
        	}
    		//
            path += "/" + Cluster.getCurrent().getZkIndexMasterSlave();
            if(!zkClient.exists(path)){
            	zkClient.createEphemeral(path, null);
            }
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
