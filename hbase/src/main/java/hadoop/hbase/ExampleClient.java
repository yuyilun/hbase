package hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

/**
 * 使用例子
 * @author yu100
 *
 */
public class ExampleClient {
	
	private static final String TABLE_NAME = "table-name";
	private static final String DEFAULT_COLUMN_FAMILY = "column-family";
	
	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "10.10.11.11,10.10.11.16,10.10.11.17");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		//Add any necessary configuration files (hbase-site.xml, core-site.xml)
		//configuration.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
		//configuration.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
		createSchemaTables(configuration);
		modifySchema(configuration);
	}
	
	/**
	 * 创建表
	 * @param admin
	 * @param table
	 * @throws IOException
	 */
	public static void createOrOverwrite(Admin admin, TableDescriptor table) throws IOException {
		if(admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
		System.out.println(" createTable.");
	}
	
	/**
	 * 创建表模型
	 * @param config
	 * @throws IOException
	 */
	public static void createSchemaTables(Configuration config) throws IOException {
		try(
			Connection connection = ConnectionFactory.createConnection(config);
			Admin admin = connection.getAdmin())
		{	
			
			//TableDescriptor table = TableDescriptorBuilder
			//		.newBuilder(TableName.valueOf(TABLE_NAME))
			//		.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(DEFAULT_COLUMN_FAMILY.getBytes()).build())
			//		.build();
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(new HColumnDescriptor(DEFAULT_COLUMN_FAMILY).setCompressionType(Algorithm.NONE));
			System.out.print("Creating table. ");
			createOrOverwrite(admin, table);
			System.out.println(" Done.");
		}
	}
	
	
	/**
	 * 修改表模型
	 * @param config
	 * @throws IOException
	 */
	@SuppressWarnings("deprecation")
	public static void modifySchema(Configuration config) throws IOException {
		 try (Connection connection = ConnectionFactory.createConnection(config);
		         Admin admin = connection.getAdmin()) {
			 
			 TableName tableName = TableName.valueOf(TABLE_NAME);
			 if(!admin.tableExists(tableName)) {
				 System.out.println("Table does not exist.");
				 System.exit(-1);
			 }
			 
			 HTableDescriptor table = admin.getTableDescriptor(tableName);
			 // Update existing table
			 HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
			 newColumn.setCompactionCompressionType(Algorithm.GZ);
			 newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
			 admin.addColumnFamily(tableName, newColumn);
			 // Update existing column family
			 /*HColumnDescriptor existingColumn = new HColumnDescriptor(DEFAULT_COLUMN_FAMILY);
			 existingColumn.setCompactionCompressionType(Algorithm.GZ);
		     existingColumn.setMaxVersions(HConstants.ALL_VERSIONS);
		     table.modifyFamily(existingColumn);
		     admin.modifyTable(tableName, table);*/
		     
		     // Disable an existing table
		     admin.disableTable(tableName);
		     // Delete an existing column family
			 admin.deleteColumn(tableName, DEFAULT_COLUMN_FAMILY.getBytes());
			 // Delete a table (Need to be disabled first)
			 admin.deleteTable(tableName);
			 System.out.println("deleteTable ." + tableName);
		 }
	}
	
}
