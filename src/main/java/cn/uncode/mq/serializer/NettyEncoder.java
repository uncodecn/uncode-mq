package cn.uncode.mq.serializer;

import cn.uncode.mq.network.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @author : juny.ye
 */
public class NettyEncoder extends MessageToByteEncoder<Message> {
	public static final int FRAME_LEN = 4;
	/**
	 * Encode a message into a {@link ByteBuf}. This method will be called for each written message that can be handled
	 * by this encoder.
	 *
	 * @param ctx the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs to
	 * @param msg the message to encode
	 * @param out the {@link ByteBuf} into which the encoded message will be written
	 * @throws Exception is thrown if an error accour
	 */
	@Override
	protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) throws Exception {
		msg.writeToByteBuf(out);
		
		// frameLen = head + body + crc32.
//		out.writeInt(Message.HEAD_LEN + msg.bodyLength() + Message.CRC_LEN);

		// head
//		out.writeShort(msg.getMagic());
//		out.writeByte(msg.getVersion());
//		out.writeByte(msg.getType());
//		out.writeShort(msg.getReqHandlerType());
//		out.writeInt(msg.getSeqId());

		// body
//		if (msg.bodyLength() > 0)
//			out.writeBytes(msg.getBody());

		// <<head + body>> 进行 crc
//		long crc32 = DataUtils.calculateChecksum(out, out.readerIndex() + 4, out.readableBytes() - 4);

		// 最后写CRC.
//		out.writeBytes(DataUtils.uint32ToBytes(crc32));
	}

}
