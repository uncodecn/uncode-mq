package cn.uncode.mq.exception;

/**
 * @author : juny.ye
 */
public class TimeoutException extends RuntimeException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TimeoutException(String message) {
		super(message);
	}

	public TimeoutException(String message, Throwable cause) {
		super(message, cause);
	}

	public TimeoutException(Throwable cause) {
		super(cause);
	}
}
