package cn.uncode.mq.server;

import cn.uncode.mq.network.Message;

/**
 * @author : juny.ye
 */
public interface RequestHandler {
	
	short PING = 1;
	short UUID = 2;
	short FETCH = 3;
	short PRODUCER = 4;
	short REPLICA = 5;
	
	Message handler(Message request);
	
	
}
