package cn.uncode.mq.network;

import java.util.concurrent.atomic.AtomicInteger;

import cn.uncode.mq.util.DataUtils;
import io.netty.buffer.ByteBuf;

/**
 * @author : juny.ye
 */
public class Message {
	private final static AtomicInteger RequestId = new AtomicInteger(1);
	// frame ==> head:[ magic + version + type + reqType + seqId] [body]  [ crc32]
	public final static int MAGIC = 0xf11f;
	public final static int HEAD_LEN = 2 + 1 + 1 + 2 + 4;
	public final static int CRC_LEN = 4;
	public final static int BODY_MAX_LEN = 8388607 - HEAD_LEN - CRC_LEN;

	private short magic = (short) MAGIC;
	private byte version = (byte)10;
	private byte type = 0;    // CALL | REPLY | EXCEPTION
	private short reqHandlerType = 0; // FETCH | PRODUCER
	private int seqId = 0;
	private transient byte[] body = new byte[0];

	public static Message newRequestMessage() {
		Message msg = new Message();
		msg.setType(TransferType.CALL.value);
		msg.setSeqId(RequestId.getAndIncrement());
		return msg;
	}

	public static Message newResponseMessage() {
		Message msg = new Message();
		msg.setType(TransferType.REPLY.value);
		return msg;
	}

	public static Message newExceptionMessage() {
		Message msg = new Message();
		msg.setType(TransferType.EXCEPTION.value);
		return msg;
	}

	public short getMagic() {
		return magic;
	}

	public byte getVersion() {
		return version;
	}

	public void setVersion(byte version) {
		this.version = version;
	}

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

	public Message setSeqId(int seqId) {
		this.seqId = seqId;
		return this;
	}

	public int getSeqId() {
		return seqId;
	}
	
	public short getReqHandlerType() {
		return reqHandlerType;
	}

	public void setReqHandlerType(short reqHandlerType) {
		this.reqHandlerType = reqHandlerType;
	}

	public byte[] getBody() {
		return body;
	}

	public Message setBody(byte[] body) {
		this.body = body;
		return this;
	}
	
	public int serializedSize() {
        return Message.HEAD_LEN + bodyLength() + Message.CRC_LEN;
    }

	public int bodyLength() {
		return this.body == null ? 0 : this.body.length;
	}
	
	public void writeToByteBuf(ByteBuf byteBuf){
		if(byteBuf != null){
			// totalLength = head + body + crc32.
			byteBuf.writeInt(Message.HEAD_LEN + bodyLength() + Message.CRC_LEN);

			// head
			byteBuf.writeShort(getMagic());
			byteBuf.writeByte(getVersion());
			byteBuf.writeByte(getType());
			byteBuf.writeShort(getReqHandlerType());
			byteBuf.writeInt(getSeqId());
			

			// body
			if (bodyLength() > 0)
				byteBuf.writeBytes(getBody());

			// <<head + body>> 进行 crc
			long crc32 = DataUtils.calculateChecksum(byteBuf, byteBuf.readerIndex() + 4, byteBuf.readableBytes() - 4);

			// 最后写CRC.
			byteBuf.writeBytes(DataUtils.uint32ToBytes(crc32));
		}
	}
	
	public void readFromByteBuf(ByteBuf byteBuf){
		if (byteBuf != null) {
			// <<head + body>> 进行 crc
			int len = byteBuf.readableBytes() - Message.CRC_LEN;
			long crc32 = DataUtils.calculateChecksum(byteBuf, byteBuf.readerIndex(), len);

			// 读取头信息.
			int magic = byteBuf.readUnsignedShort();
			if (magic != Message.MAGIC) {
				byteBuf.discardReadBytes();
				byteBuf.release();
				throw new RuntimeException("协议Magic无效!");
			}
			byte ver = byteBuf.readByte();
			byte type = byteBuf.readByte();
			short reqHandlerType = byteBuf.readShort();
			int seqId = byteBuf.readInt();
			

			// 读取body
			int body_len = len - Message.HEAD_LEN;
			byte[] body = new byte[body_len];
			byteBuf.readBytes(body);

			// 读取最后的CRC32
			long read_crc32 = byteBuf.readUnsignedInt();
			if (read_crc32 != crc32) {
				byteBuf.discardReadBytes();
				byteBuf.release();
				throw new RuntimeException("CRC32不对");
			}

			this.setVersion(ver);
			this.setType(type);
			this.setSeqId(seqId);
			this.setReqHandlerType(reqHandlerType);
			this.setBody(body);

			byteBuf.release();
		}
	}
	
	public static Message buildFromByteBuf(ByteBuf byteBuf){
		Message message = new Message();
		message.readFromByteBuf(byteBuf);
		return message;
	}

	@Override
	public String toString() {
		return String.format("magic:%s, version:%s, type:%s, reqHandlerType:%s, seqId:%d", magic, version, type, reqHandlerType, seqId);
	}
	
	@Override
    public boolean equals(Object obj) {
        if (obj instanceof Message) {
            Message m = (Message) obj;
            return getMagic() == m.getMagic()//
                    && getVersion() == m.getVersion()//
                    && getType() == m.getType()//
                    && getReqHandlerType() == m.getReqHandlerType()//
                    && getSeqId() == m.getSeqId()
                    && bodyLength() == m.bodyLength();
        }
        return false;
    }

    
}
