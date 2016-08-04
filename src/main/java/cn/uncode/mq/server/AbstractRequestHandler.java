package cn.uncode.mq.server;

import cn.uncode.mq.config.ServerConfig;

public abstract class AbstractRequestHandler implements RequestHandler {
	
	protected ServerConfig config;
	
	public AbstractRequestHandler(ServerConfig config){
		this.config = config;
	}

	

}
