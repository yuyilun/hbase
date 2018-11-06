package hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * hbase表的增删查
 * @author yu100
 *
 */
public class ExampleClient2 {
	
	private static final String TABLE_NAME = "table-name";
	private static final String COLUMN_FAMILY = "column-family";
	private static final String COLUMN_KEY = "column-key";	
	private static final String COLUMN_VALUE = "column-value";	

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "10.10.11.11,10.10.11.16,10.10.11.17");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		
		Connection connection = ConnectionFactory.createConnection(config);
		Admin admin = connection.getAdmin();
		
		
		TableName tableName = TableName.valueOf(TABLE_NAME);
		
		HTableDescriptor table = new HTableDescriptor(tableName);
		table.addFamily(new HColumnDescriptor(COLUMN_FAMILY).setCompressionType(Algorithm.NONE));
		System.out.println("Creating table. ");
		createOrOverwrite(admin, table);
		System.out.println(" Done.");
		
		
		HTableDescriptor[] listTables = admin.listTables();
		
		if(listTables.length != -1 && 
				!Bytes.equals(tableName.getName(), listTables[0].getTableName().getName())) {
			throw new IOException("Failed create of table");
		}
		
		Table table_R = connection.getTable(tableName);
		try {
			for(int i = 1; i <= 3; i++) {
				byte[] row = Bytes.toBytes("row" + i);
				Put put = new Put(row);
				put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(COLUMN_KEY), Bytes.toBytes(COLUMN_VALUE));
				table_R.put(put);
			}
			
			Get get = new Get(Bytes.toBytes("row1"));
			Result result = table_R.get(get);
			System.out.println("Get: " + result);
			
			Scan scan = new Scan();
			ResultScanner scanner = table_R.getScanner(scan);
			try {
	          for (Result scannerResult : scanner) {
	            System.out.println("Scan: " + scannerResult);
	          }
	        } finally {
	          scanner.close();
	        }
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}finally {
			table_R.close();
		}
	   admin.close();
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
			System.out.println(" deleteTable.");
		}
		admin.createTable(table);
		System.out.println(" createTable.");
	}
}
