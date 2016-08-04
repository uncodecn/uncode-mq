package cn.uncode.mq.store;

public interface TopicQueueIndex {

	String MAGIC = "umqv.1.0";
	int INDEX_SIZE = 32;
	int READ_NUM_OFFSET = 8;
	int READ_POS_OFFSET = 12;
	int READ_CNT_OFFSET = 16;
	int WRITE_NUM_OFFSET = 20;
	int WRITE_POS_OFFSET = 24;
	int WRITE_CNT_OFFSET = 28;

	int getReadNum();

	int getReadPosition();

	int getReadCounter();

	int getWriteNum();

	int getWritePosition();

	int getWriteCounter();

	void putMagic();

	void putWritePosition(int writePosition);

	void putWriteNum(int writeNum);

	void putWriteCounter(int writeCounter);

	void putReadNum(int readNum);

	void putReadPosition(int readPosition);

	void putReadCounter(int readCounter);

	void reset();

	void sync();

	void close();

}