package cn.uncode.mq.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.network.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.collection.IntObjectMap;

/**
 * @author : juny.ey
 */
class NettyServerHandler extends SimpleChannelInboundHandler<Message> {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(NettyServerHandler.class);
	
	private final IntObjectMap<RequestHandler> requestHandlers;

	public NettyServerHandler(IntObjectMap<RequestHandler> requestHandlers) {
		this.requestHandlers = requestHandlers;
	}


	@Override
	protected void channelRead0(ChannelHandlerContext ctx, Message request) throws Exception {
		RequestHandler handler = requestHandlers.get((int)request.getReqHandlerType());
		if (handler == null) {
			Message appMessage = Message.newExceptionMessage();
			appMessage.setSeqId(request.getSeqId());
			ctx.writeAndFlush(appMessage);
		} else {
			Message response = handler.handler(request);
			if(request.getReqHandlerType() == RequestHandler.FETCH){
				if(response.getBody().length > 0){
					LOGGER. info(String.format("=====>send a request to channel <%s> success, type:%s", ctx.channel(), typeValue(request.getReqHandlerType())));
				}
			}else if(request.getReqHandlerType() == RequestHandler.PRODUCER){
				LOGGER. info(String.format("=====>send a request to channel <%s> success, type:%s", ctx.channel(), typeValue(request.getReqHandlerType())));
			}
			ctx.writeAndFlush(response);
		}
	}
	
	
	private String typeValue(short reqHandlerType){
		String val = "";
		switch (reqHandlerType) {
		case 3:
			val = "FETCH";
			break;
		case 4:
			val = "PRODUCER";
			break;
		case 5:
			val = "REPLICA";
			break;
		default:
			break;
		}
		return val;
	}
}
