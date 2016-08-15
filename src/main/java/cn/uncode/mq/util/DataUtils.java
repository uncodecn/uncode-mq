package cn.uncode.mq.util;

import io.netty.buffer.ByteBuf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Type;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.cluster.Group;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class DataUtils {
	private static final Logger LOGGER = LoggerFactory.getLogger(DataUtils.class);
	
	public static byte[] serialize(Object object) {
		ObjectOutputStream oos = null;
		ByteArrayOutputStream baos = null;
		try {
			// 序列化
			baos = new ByteArrayOutputStream();
			oos = new ObjectOutputStream(baos);
			oos.writeObject(object);
			byte[] bytes = baos.toByteArray();
			return bytes;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static Object deserialize(byte[] bytes) {
		ByteArrayInputStream bais = null;
		try {
			// 反序列化
			bais = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(bais);
			return ois.readObject();
		} catch (Exception e) {

		}
		return null;
	}
	
	public static long calculateChecksum(ByteBuf data, int offset, int length) {
		CRC32 crc32 = new CRC32();
		try {
			if (data.hasArray()) {
				crc32.update(data.array(), data.arrayOffset() + offset, length);
			} else {
				for (int i = 0; i < length; ++i) {
					crc32.update(data.getByte(offset + i));
				}
			}
			// unsigned int
			return crc32.getValue();
		} finally {
			crc32.reset();
		}
	}
	
	public static byte[] uint32ToBytes(long value) {
		return uint32ToBytes(ByteOrder.BIG_ENDIAN, value);
	}

	public static byte[] uint32ToBytes(ByteOrder order, long value) {
		byte[] buf = new byte[4];
		uint32ToBytes(order, buf, 0, value);
		return buf;
	}

	private static void uint32ToBytes(ByteOrder order, byte[] buf, int offset, long value) {
		if (offset + 4 > buf.length) throw new IllegalArgumentException("buf no has 4 byte space");
		if (order == ByteOrder.BIG_ENDIAN) {
			buf[offset + 0] = (byte) (0xff & (value >>> 24));
			buf[offset + 1] = (byte) (0xff & (value >>> 16));
			buf[offset + 2] = (byte) (0xff & (value >>> 8));
			buf[offset + 3] = (byte) (0xff & (value));
		} else {
			buf[offset + 0] = (byte) (0xff & (value));
			buf[offset + 1] = (byte) (0xff & (value >>> 8));
			buf[offset + 2] = (byte) (0xff & (value >>> 16));
			buf[offset + 3] = (byte) (0xff & (value >>> 24));
		}
	}
	
	
	static class TimestampTypeAdapter implements JsonSerializer<Timestamp>, JsonDeserializer<Timestamp>{   
	    public JsonElement serialize(Timestamp src, Type arg1, JsonSerializationContext arg2) {   
	    	DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");   
	        String dateFormatAsString = format.format(new Date(src.getTime()));   
	        return new JsonPrimitive(dateFormatAsString);   
	    }   
	  
	    public Timestamp deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {   
	        if (!(json instanceof JsonPrimitive)) {   
	            throw new JsonParseException("The date should be a string value");   
	        }   
	  
	        try {   
	        	DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");   
	            Date date = (Date) format.parse(json.getAsString());   
	            return new Timestamp(date.getTime());   
	        } catch (Exception e) {   
	            throw new JsonParseException(e);   
	        }   
	    }
	}
	
	private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Timestamp.class,new TimestampTypeAdapter()).setDateFormat("yyyy-MM-dd HH:mm:ss").create();
	
	public static String brokerGroup2Json(Group group){
		return GSON.toJson(group);
	}
	
	public static Group json2BrokerGroup(String json){
		return GSON.fromJson(json, Group.class);
	}

	public static void printStackTrace() {
		LOGGER.info("printStackTrace run");
		try{
	        StackTraceElement[] stackElements = new Throwable().getStackTrace();
	        if(stackElements != null) {
	            for(int i = 0, leng=stackElements.length; i < leng && i<200; i++) {
	            	LOGGER.info(""+ stackElements[i]); 
	            }
	        }
		}finally{
			
		}
    }
}
