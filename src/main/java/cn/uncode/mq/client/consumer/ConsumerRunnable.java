package cn.uncode.mq.client.consumer;

import java.net.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.config.ServerConfig;

public class ConsumerRunnable extends Thread{
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ConsumerRunnable.class);
	
	private boolean closed = false;
	
	ConsumerRunnable(ServerConfig config) throws ConnectException{
		super("uncode-mq-consumer");
		Consumer.getInstance().connect(config);
	}
	
    @Override
    public void run() {
	    try {
	        while (!closed) {
	        	try{
	        		Consumer.fetch();
	        	}catch (Exception e) {
	    	    	LOGGER.error("consumer fetch error ", e);
	    	    }
	        	Thread.sleep(500);
	        }
	    }catch (InterruptedException e) {
	    	LOGGER.error("error in consumer runnable ", e);
	    }
    }
    
    public void start(){
    	super.start();
    }
	
	public void close(){
		closed = true;
		this.interrupt();
	}
	
	
	
	
	

}
