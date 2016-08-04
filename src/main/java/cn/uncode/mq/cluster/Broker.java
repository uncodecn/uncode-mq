
package cn.uncode.mq.cluster;

import java.io.Serializable;

/**
 * messages store broker
 *
 * @author juny.ye
 */
public class Broker implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

    /**
     * broker hostname
     */
    private final String host;

    /**
     * broker port
     */
    private final int port;
    
    private String shost;

    /**
     * create a broker
     *
     * @param host      broker hostname
     * @param port      broker port
     * @param autocreated auto-create new topics
     */
    public Broker(String host, int port) {
        this.host = host;
        this.port = port;
    }
    
    public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getShost() {
		return shost;
	}

	public void setShost(String shost) {
		this.shost = shost;
	}

	@Override
    public String toString() {
        return String.format("%s:%s:%s",  host, port, shost);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Broker broker = (Broker) o;

        if (port != broker.port) return false;
        if (host != null ? !host.equals(broker.host) : broker.host != null) return false;
        if (shost != null ? !shost.equals(broker.shost) : broker.shost != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + (host != null ? host.hashCode() : 0);
        result = 31 * result + port;
        result = 31 * result + (shost != null ? shost.hashCode() : 0);
        return result;
    }

    

}
