package cn.uncode.mq.server.handlers;

import cn.uncode.mq.network.Message;
import cn.uncode.mq.server.RequestHandler;

/**
 * 参照 twitter 的id生成算法Id.
 *
 * @author : juny.ye
 */
public class UuidRequestHandler implements RequestHandler {
	private static final long WBEGIN = 1288834974657L;
	private long id = 0;   //  6bit
	private long cur = 0L; // 41bit
	private long seq = 0L; // 16bit

	public UuidRequestHandler(long id) {
		this.id = ((id & 0x3f) << 57);
		this.cur = System.currentTimeMillis() - WBEGIN;
	}

	@Override
	public Message handler(Message request) {
		Message response = Message.newResponseMessage();
		response.setSeqId(request.getSeqId());
//		response.setBody(ByteUtils.toBytes(nextId()));
		return response;
	}

	public synchronized long nextId() {
		long timestamp = timeGen();

		if (timestamp == this.cur) {
			this.seq = (this.seq + 1) & 0xffff;
			if (this.seq == 0) {
				// wait timeGen() > timestamp.
				timestamp = this.waitNextMillis(timestamp);
			}
		} else {
			this.seq = 0;
		}
		this.cur = timestamp;
		return id | (timestamp << 16) | seq;
	}

	private long timeGen() {
		return System.currentTimeMillis() - WBEGIN;
	}

	private long waitNextMillis(long lastTimestamp) {
		long timestamp = timeGen();
		while (timestamp <= lastTimestamp) {
			timestamp = timeGen();
		}
		return timestamp;
	}

	public static long svrId(long v) {
		return (v >>> 57) & 0x3f;
	}

	public static long timeId(long v) {
		return ((v >>> 16) & 0x1ffffffffffL) + WBEGIN;
	}

	public static long seqId(long v) {
		return v & 0xffff;
	}


	
}
