package hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

/**
 *hbase 使用例子
 *
 */
public class App 
{
	
	private static Configuration conf = HBaseConfiguration.create();
	private static Table table= null;
	
	static {
		
    	try {
    		Connection connection = ConnectionFactory.createConnection(conf);
			table = connection.getTable(TableName.valueOf("table-name"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
    public static void main( String[] args ) throws Exception
    {
    	put();
    	get();
    	delete();
    	Scans();
    	close();
        System.out.println( "Hello World!" );
    }
    
    /**
     * 新增和修改
     * @throws Exception
     */
    private static void put() throws Exception {
    	Put put = new Put("row-key".getBytes());
    	put.addColumn("column-family".getBytes(), "column".getBytes(), "value".getBytes());
    	table.put(put);
    }
    
    /**
     * 查询
     * @throws Exception
     */
    private static void get() throws Exception {
    	Get get = new Get("row-key".getBytes());
    	get.addColumn("column-family".getBytes(), "column".getBytes());
    	table.get(get);
    }
    
    /**
     * 删除
     * @throws Exception
     */
    private static void delete() throws Exception {
    	Delete delete = new Delete("row-key".getBytes());
    	delete.addColumn("column-family".getBytes(), "column".getBytes());
    	table.delete(delete);	
    }
    
    /**
     * 扫描
     * @throws Exception
     */
    private static void Scans() throws Exception {
    	Scan scan = new Scan();
    	scan.addColumn("column-family".getBytes(), "column".getBytes());
    	scan.setRowPrefixFilter("row-key".getBytes());
    	
    	ResultScanner resultScanner = table.getScanner(scan);
    	
    	for(Result r = resultScanner.next(); r != null ; r = resultScanner.next()) {
    		System.out.println(r.value());
    	}
    }
    
    /**
     * 关闭table句柄
     * @throws Exception
     */
    private static void close() throws Exception {
    	table.close();
    }
    
}
