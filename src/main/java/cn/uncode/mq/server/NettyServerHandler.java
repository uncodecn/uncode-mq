package cn.uncode.mq.server;

import cn.uncode.mq.network.Message;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.collection.IntObjectMap;

/**
 * @author : juny.ey
 */
class NettyServerHandler extends SimpleChannelInboundHandler<Message> {
	
	private final IntObjectMap<RequestHandler> requestHandlers;

	public NettyServerHandler(IntObjectMap<RequestHandler> requestHandlers) {
		this.requestHandlers = requestHandlers;
	}


	@Override
	protected void messageReceived(ChannelHandlerContext ctx, Message request) throws Exception {
		RequestHandler handler = requestHandlers.get((int)request.getReqHandlerType());
		if (handler == null) {
			Message appMessage = Message.newExceptionMessage();
			appMessage.setSeqId(request.getSeqId());
			ctx.writeAndFlush(appMessage);
		} else {
			Message response = handler.handler(request);
			ctx.writeAndFlush(response);
		}
		
	}
}
