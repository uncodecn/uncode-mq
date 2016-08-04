package cn.uncode.mq.network;

import java.io.Serializable;

public class Backup implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -330577267934364039L;
	
	private String queueName;
	private int slaveWriteNum;       // 20  写索引文件号
    private int slaveWritePosition;  // 24  写索引位置
    private int slaveWriteCounter;   // 28 总写入数量
    
    
    
	public Backup(String queueName, int slaveWriteNum, int slaveWritePosition, int slaveWriteCounter) {
		super();
		this.queueName = queueName;
		this.slaveWriteNum = slaveWriteNum;
		this.slaveWritePosition = slaveWritePosition;
		this.slaveWriteCounter = slaveWriteCounter;
	}
	
	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public int getSlaveWriteNum() {
		return slaveWriteNum;
	}
	public void setSlaveWriteNum(int slaveWriteNum) {
		this.slaveWriteNum = slaveWriteNum;
	}
	public int getSlaveWritePosition() {
		return slaveWritePosition;
	}
	public void setSlaveWritePosition(int slaveWritePosition) {
		this.slaveWritePosition = slaveWritePosition;
	}
	public int getSlaveWriteCounter() {
		return slaveWriteCounter;
	}
	public void setSlaveWriteCounter(int slaveWriteCounter) {
		this.slaveWriteCounter = slaveWriteCounter;
	}
    
    public String toString(){
    	return String.format("%s:rnum%d,rposition%d,rcounter%d", queueName, slaveWriteNum, slaveWritePosition, slaveWriteCounter);
    }
    

}
