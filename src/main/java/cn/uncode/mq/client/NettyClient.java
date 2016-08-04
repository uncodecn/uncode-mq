package cn.uncode.mq.client;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.exception.SendRequestException;
import cn.uncode.mq.exception.TimeoutException;
import cn.uncode.mq.network.Message;
import cn.uncode.mq.serializer.NettyDecoder;
import cn.uncode.mq.serializer.NettyEncoder;
import cn.uncode.mq.zk.ZkClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

/**
 * @author : juny.ye
 */
public abstract class NettyClient {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

	private final Bootstrap bootstrap = new Bootstrap();

	private final EventLoopGroup eventLoopGroupWorker;
	private DefaultEventExecutorGroup defaultEventExecutorGroup;
	private Channel channel = null;
	// 缓存所有对外请求
	protected final ConcurrentHashMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>(256);
	
	protected ZkClient zkClient;
	protected boolean connected;
	
	public NettyClient() {
		this.eventLoopGroupWorker = new NioEventLoopGroup(1);
		this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(16);
	}
	
	public void initZkClient(ServerConfig config){
		if(config.getEnableZookeeper()){
			String authString = config.getZkUsername() + ":"+ config.getZkPassword();
			this.zkClient = new ZkClient(config.getZkConnect(), authString,
					config.getZkSessionTimeoutMs(), 
					config.getZkConnectionTimeoutMs());
		}
	}
	
	public void open(String host, int port) throws RuntimeException{
		Bootstrap handler = this.bootstrap.group(this.eventLoopGroupWorker)
				.channel(NioSocketChannel.class)
				.option(ChannelOption.TCP_NODELAY, true)
				.option(ChannelOption.SO_KEEPALIVE, false)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					public void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline().addLast(
								defaultEventExecutorGroup,
								//new LoggingHandler("client", LogLevel.INFO),
								new NettyDecoder(),
								new NettyEncoder(),
								new NettyClientHandler()
						);
					}
				});
		ChannelFuture channelFuture = handler.connect(host, port);
		this.channel = channelFuture.channel();
		try {
			channelFuture.sync();
			LOGGER.info("connect {}:{} ok.", host, port);
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		connected = true;
	}
	
	public void stop() {
		LOGGER.info("close channel:{}", this.channel);
		this.eventLoopGroupWorker.shutdownGracefully();

		if (this.defaultEventExecutorGroup != null) {
			this.defaultEventExecutorGroup.shutdownGracefully();
		}
		
		if(this.channel != null){
			this.channel.close();
		}
		connected = false;
		LOGGER.info("close channel:{} ok.", this.channel);
	}

	class NettyClientHandler extends SimpleChannelInboundHandler<Message> {

		@Override
		protected void messageReceived(ChannelHandlerContext ctx, Message msg) throws Exception {
			int id = msg.getSeqId();
			ResponseFuture responseFuture = responseTable.get(id);
			if (responseFuture != null) {
				LOGGER.debug("receive request id:{} response data.", id);

				responseFuture.setResponse(msg);
				responseFuture.release();           // 发出通知,可以读取response了.

				responseTable.remove(id);
			} else {
				LOGGER.warn("receive request id:{} response, but it's not in.", id);
			}
			
		}
	}

	public Message write(final Message request) throws TimeoutException, SendRequestException {

		final ResponseFuture responseFuture = new ResponseFuture(request.getSeqId());
		responseTable.put(responseFuture.getId(), responseFuture);

		this.channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
			@Override
			public void operationComplete(ChannelFuture future) throws Exception {
				if (future.isSuccess()) {
					responseFuture.setIsOk(true);
					return;
				} else {
					responseFuture.setIsOk(false);
				}

				// 写入失败了,就从缓存中移掉这个请求
				responseTable.remove(responseFuture.getId(), responseFuture);

				responseFuture.setCause(future.cause());
				responseFuture.setResponse(null);
				LOGGER.warn("send a request to channel <{}> failed.\nREQ:{}", future.channel(), request);
			}
		});

		Message response = null;
		try {
			response = responseFuture.waitResponse(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// ignore e;
		}
		if (null == response) {
			if (responseFuture.isOk()) {
				throw new TimeoutException(
						String.format("wait response on the channel <%s> timeout 10 (s).", this.channel),
						responseFuture.getCause()
				);
			} else {
				throw new SendRequestException(
						String.format("send request to the channel <%s> failed.", this.channel),
						responseFuture.getCause());
			}
		} else {
			LOGGER.debug(String.format("send a request to channel <%s> success.\nREQ:%s\nRES:%s", this.channel, request, response));
		}

		return response;
	}
	
	public boolean isConnected() {
		return connected;
	}

	public void setConnected(boolean connected) {
		this.connected = connected;
	}
	
	public void connect(Properties config) throws ConnectException{
		ServerConfig serverConfig = new ServerConfig(config);
		connect(serverConfig);
	}
	
	public void connect(String configFileName)throws ConnectException {
        File mainFile = null;
		try {
			mainFile = new File(configFileName).getCanonicalFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        if (!mainFile.isFile() || !mainFile.exists()) {
            System.err.println(String.format("ERROR: Main config file not exist => '%s', copy one from 'conf/server.properties.sample' first.", mainFile.getAbsolutePath()));
            System.exit(2);
        }
        final ServerConfig config = new ServerConfig(configFileName);
        connect(config);
    }
	

	///////////////////////////////////////////////
	public abstract void connect(ServerConfig config) throws ConnectException;
	
	
	public abstract boolean reConnect();
	

}
