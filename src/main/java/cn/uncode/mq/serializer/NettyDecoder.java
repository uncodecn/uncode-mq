package cn.uncode.mq.serializer;

import cn.uncode.mq.network.Message;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

/**
 * @author : juny.ye
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {

	public NettyDecoder() {
		super(Integer.MAX_VALUE, 0, NettyEncoder.FRAME_LEN, 0, NettyEncoder.FRAME_LEN);
	}


	@Override
	protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
		ByteBuf frame = (ByteBuf) super.decode(ctx, in);
		if (null == frame) {
			return null;
		}
		Message msg = new Message();
		msg.readFromByteBuf(frame);
		
		
		// <<head + body>> 进行 crc
//		int len = frame.readableBytes() - Message.CRC_LEN;
//		long crc32 = DataUtils.calculateChecksum(frame, frame.readerIndex(), len);

		// 读取头信息.
//		int magic = frame.readUnsignedShort();
//		if (magic != Message.MAGIC) {
//			frame.discardReadBytes();
//			frame.release();
//			throw new Exception("协议Magic无效!");
//		}
//		byte ver = frame.readByte();
//		byte type = frame.readByte();
//		short reqHandlerType = frame.readShort();
//		int seqId = frame.readInt();

		// 读取body
//		int body_len = len - Message.HEAD_LEN;
//		byte[] body = new byte[body_len];
//		frame.readBytes(body);

		// 读取最后的CRC32
//		long read_crc32 = frame.readUnsignedInt();
//		if (read_crc32 != crc32) {
//			frame.discardReadBytes();
//			frame.release();
//			throw new Exception("CRC32不对");
//		}
//
//		Message msg = new Message();
//		msg.setVersion(ver);
//		msg.setType(type);
//		msg.setReqHandlerType(reqHandlerType);
//		msg.setSeqId(seqId);
//		msg.setBody(body);
//
//		frame.release();
		
		return msg;
	}
}
