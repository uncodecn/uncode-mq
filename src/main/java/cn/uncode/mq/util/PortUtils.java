package cn.uncode.mq.util;

import java.io.IOException;
import java.net.ServerSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ━━━━━━神兽出没━━━━━━
 * 　　　┏┓　　　┏┓
 * 　　┏┛┻━━━┛┻┓
 * 　　┃　　　　　　　┃
 * 　　┃　　　━　　　┃
 * 　　┃　┳┛　┗┳　┃
 * 　　┃　　　　　　　┃
 * 　　┃　　　┻　　　┃
 * 　　┃　　　　　　　┃
 * 　　┗━┓　　　┏━┛
 * 　　　　┃　　　┃神兽保佑, 永无BUG!
 * 　　　　┃　　　┃Code is far away from bug with the animal protecting
 * 　　　　┃　　　┗━━━┓
 * 　　　　┃　　　　　　　┣┓
 * 　　　　┃　　　　　　　┏┛
 * 　　　　┗┓┓┏━┳┓┏┛
 * 　　　　　┃┫┫　┃┫┫
 * 　　　　　┗┻┛　┗┻┛
 * ━━━━━━感觉萌萌哒━━━━━━
 * Module Desc:uncode-mq
 * User: juny.ye
 * Date: 2016/05/23
 */
public class PortUtils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(PortUtils.class);

    /**
     * check available port on the machine<br/>
     * This will check the next port if the port is already used.
     * @param port the checking port
     * @return a available port
     */
    public static int checkAvailablePort(int port) {
//        while (port < 65500) {
        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
        	throw new RuntimeException(e.getMessage() + String.format(" port %d", port), e);
        } finally {
            if(serverSocket != null){
            	try {
                    serverSocket.close();
                } catch (IOException e) {
                	LOGGER.error(e.getMessage(),e);
                }
            }
        }
//        }
        return port;
    }

    public static void main(String[] args) {
        int port = checkAvailablePort(80);
        System.out.println("The available port is " + port);
    }
}
