package cn.uncode.mq.client;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import cn.uncode.mq.network.Message;

/**
 * @author : juny.ye
 * 
 */
class ResponseFuture {
	
	private final CountDownLatch downLatch = new CountDownLatch(1);
	// 请求流水号
	private final int id;
	// 请求是否已经写入成功.
	private volatile boolean isOk;
	// 返回
	private volatile Message response;
	// 出错原因
	private Throwable cause;

	public ResponseFuture(int id) {
		this.id = id;
	}

	public int getId() {
		return id;
	}

	public void setCause(Throwable cause) {
		this.cause = cause;
	}

	public Throwable getCause() {
		return cause;
	}

	public void setIsOk(boolean isOk) {
		this.isOk = isOk;
	}

	public boolean isOk() {
		return isOk;
	}

	public void setResponse(Message response) {
		this.response = response;
	}

	public Message getResponse() {
		return response;
	}

	public Message waitResponse(long timeOut, TimeUnit timeUnit) throws InterruptedException {
		downLatch.await(timeOut, timeUnit);
		return this.response;
	}

	public void release() {
		downLatch.countDown();
	}
}
