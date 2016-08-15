package cn.uncode.mq.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.util.Closer;

public class Config {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Config.class);
	
	protected final Properties properties;
	
	public Config(Properties props){
		this.properties = props;
	}
	
	public Config(String filename){
		File cfg = new File("./"+filename);
		properties = new Properties();
        FileInputStream fis = null;
        try {
        	
    		if (cfg.exists()) {
    			fis = new FileInputStream(cfg);
                properties.load(fis);
            }else{
            	throw new RuntimeException("Config file is null");
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            Closer.closeQuietly(fis);
        }
	}
	
	public Config(File cfg){
		properties = new Properties();
        FileInputStream fis = null;
        try {
    		if (cfg.exists()) {
    			fis = new FileInputStream(cfg);
                properties.load(fis);
            }else{
            	throw new RuntimeException("Config file is null");
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            Closer.closeQuietly(fis);
        }
	}
	 /**
     * Get a string property, or, if no such property is defined, return
     * the given default value
     *
     * @param name         the key in the properties
     * @param defaultValue the default value if the key not exists
     * @return value in the props or defaultValue while name not exist
     */
    public String getString(String name, String defaultValue) {
        return properties.containsKey(name) ? properties.getProperty(name) : defaultValue;
    }

    public String getString(String name) {
        if (properties.containsKey(name)) {
            return properties.getProperty(name);
        }
        throw new IllegalArgumentException("Missing required property '" + name + "'");
    }

    public int getInt(String name) {
        if (properties.containsKey(name)) {
            return getInt(name, -1);
        }
        throw new IllegalArgumentException("Missing required property '" + name + "'");
    }

    public int getInt(String name, int defaultValue) {
        return getIntInRange(name, defaultValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public int getIntInRange(String name, int defaultValue, int min, int max) {
        int v = defaultValue;
        if (properties.containsKey(name)) {
            v = Integer.valueOf(properties.getProperty(name));
        }
        if (v >= min && v <= max) {
            return v;
        }
        throw new IllegalArgumentException(name + " has value " + v + " which is not in the range");
    }

    public boolean getBoolean(String name, boolean defaultValue) {
        if (!properties.containsKey(name)) return defaultValue;
        return "true".equalsIgnoreCase(properties.getProperty(name));
    }
    
    /**
     * Get a property of type java.util.Properties or return the default if
     * no such property is defined
     */
    public Properties getProps(String name, Properties defaultProperties) {
        final String propString = properties.getProperty(name);
        if (propString == null) return defaultProperties;
        String[] propValues = propString.split(",");
        if (propValues.length < 1) {
            throw new IllegalArgumentException("Illegal format of specifying properties '" + propString + "'");
        }
        Properties properties = new Properties();
        for (int i = 0; i < propValues.length; i++) {
            String[] prop = propValues[i].split("=");
            if (prop.length != 2) throw new IllegalArgumentException("Illegal format of specifying properties '" + propValues[i] + "'");
            properties.put(prop[0], prop[1]);
        }
        return properties;
    }

    public Map<String, Integer> getCSVMap(String value, String exceptionMsg, String successMsg) {
        Map<String, Integer> map = new LinkedHashMap<String, Integer>();
        if (value == null || value.trim().length() < 3) return map;
        for (String one : value.trim().split(",")) {
            String[] kv = one.split(":");
            //FIXME: force positive number
            map.put(kv[0].trim(), Integer.valueOf(kv[1].trim()));
        }
        return map;
    }
    
    protected int get(String name,int defaultValue) {
        return getInt(name, defaultValue);
    }
    
    protected String get(String name,String defaultValue) {
        return getString(name, defaultValue);
    }
    
    
    public static List<String> getCSVList(String csvList) {
        if (csvList == null || csvList.length() == 0) return Collections.emptyList();
        List<String> ret = new ArrayList<String>(Arrays.asList(csvList.split(",")));
        Iterator<String> iter = ret.iterator();
        while (iter.hasNext()) {
            final String next = iter.next();
            if (next == null || next.length() == 0) {
                iter.remove();
            }
        }
        return ret;
    }
    
    /**
     * create an instance from the className
     *
     * @param className full class name
     * @return an object or null if className is null
     */
    @SuppressWarnings("unchecked")
    public <E> E getObject(String className) {
        if (className == null) {
            return (E) null;
        }
        try {
            return (E) Class.forName(className).newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
    
    
    
    
}
