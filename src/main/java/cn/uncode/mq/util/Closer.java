package cn.uncode.mq.util;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Closer{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Closer.class);
	
	
    public static void closeQuietly(java.io.Closeable closeable, Logger logger) {
        if (closeable == null) return;
        try {
            closeable.close();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }
    
    public static void closeQuietly(java.io.Closeable closeable){
    	closeQuietly(closeable, LOGGER);
    }



}
