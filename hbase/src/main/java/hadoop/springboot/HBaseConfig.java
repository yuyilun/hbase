package hadoop.springboot;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * HBase相关配置
 * @author yu100
 *
 */
@Configuration
public class HBaseConfig {
	
	@Value("${HBase.nodes}")
	private String nodes;
	
	@Value("${HBase.maxsize}")
	private String maxsize;
	
	@Bean
	public HBaseService getHbaseService() {
		org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum",nodes );
		configuration.set("hbase.client.keyvalue.maxsize",maxsize);
		//configuration.set("hbase.zookeeper.quorum", "10.10.11.11,10.10.11.16,10.10.11.17");
		//configuration.set("hbase.zookeeper.property.clientPort", "2181");
		return new HBaseService(configuration);
		
	}
	

}
