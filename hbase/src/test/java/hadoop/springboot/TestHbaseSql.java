package hadoop.springboot;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import hadoop.RecorddataApplication;

/**
 * 测试Hbase SQL
 * @author yu100
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = RecorddataApplication.class)
@WebAppConfiguration
public class TestHbaseSql {
	
    @Autowired
    private HBaseService hBaseService;

    /**
     * 测试删除、创建表
     */
    @Test
    public void testCreateTable() {
        //删除表
    	hBaseService.deleteTable("test_base");

        //创建表
        hBaseService.createTableBySplitKeys("test_base", Arrays.asList("f","back"),hBaseService.getSplitKeys(null));

        //插入三条数据
        hBaseService.putData("test_base","66804_000001","f",
        		new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},
        		new String[]{"40866","mob_3","0.9416","0.0000","12.2293","null"});
        hBaseService.putData("test_base","66804_000002","f",
        		new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},
        		new String[]{"40866","idno_prov","0.9317","0.0000","9.8679","null"});
        hBaseService.putData("test_base","66804_000003","f",
        		new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},
        		new String[]{"40866","education","0.8984","0.0000","25.5649","null"});

        //查询数据
        //1. 根据rowKey查询
        Map<String,String> result1 = hBaseService.getRowData("test_base","66804_000001");
        System.out.println("+++++++++++根据rowKey查询+++++++++++");
        result1.forEach((k,value) -> {
            System.out.println(k + "---" + value);
        });
        System.out.println();

        //精确查询某个单元格的数据
        String str1 = hBaseService.getColumnValue("test_base","66804_000002","f","varName");
        System.out.println("+++++++++++精确查询某个单元格的数据+++++++++++");
        System.out.println(str1);
        System.out.println();

        //2. 遍历查询
        Map<String,Map<String,String>> result2 = hBaseService.getResultScanner("test_base");
        System.out.println("+++++++++++遍历查询+++++++++++");
        result2.forEach((k,value) -> {
            System.out.println(k + "---" + value);
        });
    }
    
    /**
     * 测试指定startRowKey和stopRowKey的查询
     */
    @Test
    public void testSelectByStartStopRowKey() {
    	
    	Map<String, Map<String, String>> resultScanner = hBaseService.getResultScanner("test_base", "66804_000002", "66804_000004");
    	
    	resultScanner.forEach((rowKey,columnMap) -> {
    		  System.out.println("");
    		  System.out.println("rowKey:" + rowKey);
              System.out.println("+++++++++++行数据+++++++++++");
              columnMap.forEach((key,value) -> {
            	  System.out.println(key + "---" + value);
              });
    		
    	});
    	System.out.println("-----------------------");
    }
    
    /**
     * 测试获取所有表名
     */
    @Test
    public void testGetTableName() {
    	List<String> result = hBaseService.getAllTableNames();
        result.forEach(System.out::println);
    }
    
    /**
     * 测试获取指定单元格多个版本的数据
     * 注意：
     * 因为HBase默认只保存一个版本，所以这里看不出效果。
     */
    @Test
    public void testGetColumnValuesByVersion() {
    	
    	hBaseService.setColumnValue("test_base", "66804_000002", "f", "varName", "aa");
    	hBaseService.setColumnValue("test_base", "66804_000002", "f", "varName", "bb");
    	hBaseService.setColumnValue("test_base", "66804_000002", "f", "varName", "cc");
    	hBaseService.setColumnValue("test_base", "66804_000002", "f", "varName", "dd");
    	hBaseService.setColumnValue("test_base", "66804_000002", "f", "varName", "ee");
    	hBaseService.setColumnValue("test_base", "66804_000002", "f", "varName", "ff");
    	
    	List<String> columnValuesByVersion = hBaseService.
    			getColumnValuesByVersion("test_base", "66804_000002", "f", "varName", 4);
    	columnValuesByVersion.forEach(System.out::println);
    }
    
    /**
     * 测试根据行键过滤器查询数据
     * 前缀过滤器
     */
    @Test
    public void testGetResultScannerPrefixFilter() {
    	hBaseService.putData("test_base","111","f",
    			new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},
    			new String[]{"111","111","111","111","111","null"});
    	hBaseService.putData("test_base","112","f",
    			new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},
    			new String[]{"112","112","112","112","112","null"});
    	hBaseService.putData("test_base","211","f",
    			new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},
    			new String[]{"112","112","112","112","112","null"});

    	Map<String, Map<String, String>> result = hBaseService.
    			getResultScannerPrefixFilter("test_base", "11");
    	result.forEach((rowKey,columnMap) -> {
            System.out.println("-----------------------");
            System.out.println("rowKey:" + rowKey);
            System.out.println("+++++++++++行数据+++++++++++");
            columnMap.forEach((k,value) -> {
                System.out.println(k + "---" + value);
            });
            System.out.println("-----------------------");
    	});
    }
    /**
     * 测试根据列名过滤器查询数据
     * 前缀过滤器
     */
    @Test
    public void testGetResultScannerColumnPrefixFilter() {
    	
    	hBaseService.putData("test_base","211","f",new String[]{"project_id"},new String[]{"11111"});
    	hBaseService.putData("test_base","211","f",new String[]{"var_name1"},new String[]{"111"});
    	hBaseService.putData("test_base","212","f",new String[]{"var_name2"},new String[]{"112"});
    	hBaseService.putData("test_base","212","f",new String[]{"1var_name2"},new String[]{"112"});
    	
    	 Map<String,Map<String,String>> result = hBaseService.
    			 getResultScannerColumnPrefixFilter("test_base","var_name");
         result.forEach((rowKey,columnMap) -> {
             System.out.println("-----------------------");
             System.out.println("rowKey:" + rowKey);
             System.out.println("+++++++++++行数据+++++++++++");
             columnMap.forEach((k,value) -> {
                 System.out.println(k + "---" + value);
             });
             System.out.println("-----------------------");
         });
    	
    }
    /**
     * 测试查询行键中包含特定字符的数据
     */
    @Test
    public void testGetResultScannerRowFilter() {
    	hBaseService.putData("test_base","abc666666def","f",
    			new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},
    			new String[]{"111","abc6666def","111","111","111","null"});
    	hBaseService.putData("test_base","cba666666fed","f",
    			new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},
    			new String[]{"112","cba6666fed","112","112","112","null"});
    	hBaseService.putData("test_base","666666abcfed","f",
    			new String[]{"project_id","varName","coefs","pvalues","tvalues","create_time"},
    			new String[]{"112","cba6666fed","112","112","112","null"});

		 Map<String,Map<String,String>> result = hBaseService.
				 getResultScannerRowFilter("test_base","666666");
	     result.forEach((rowKey,columnMap) -> {
	         System.out.println("-----------------------");
	         System.out.println("rowKey:" + rowKey);
	         System.out.println("+++++++++++行数据+++++++++++");
	         columnMap.forEach((k,value) -> {
	             System.out.println(k + "---" + value);
	         });
	         System.out.println("-----------------------");
	     });
    	
    }
    /**
     * 测试删除指定的列
     */
    @Test
    public void testDeleteColumn() {
    	
    	hBaseService.setColumnValue("test_base", "66804_000002", "f", "xxx", "123");
    	String str = hBaseService.getColumnValue("test_base","66804_000002","f","xxx");
        System.out.println("第一次取值：" + str);
    	
      //删除测试列
        hBaseService.deleteColumn("test_base","66804_000002","f","xxx");
    	
        //再次取值
        String str2 = hBaseService.getColumnValue("test_base","66804_000002","f","xxx");
        System.out.println("第二次取值：" + str2);
    }
    
    /**
     * 测试删除指定的行
     */
    @Test
    public void testDeleteRow(){
        //取值
        Map<String,String> result1 = hBaseService.getRowData("test_base","66804_000003");
        System.out.println("第一次取值输出：");
        result1.forEach((k,value) -> {
            System.out.println(k + "---" + value);
        });

        //删除测试行
        hBaseService.deleteRow("test_base","66804_000003");

        //再次取值
        Map<String,String> result2 = hBaseService.getRowData("test_base","66804_000003");
        System.out.println("第二次取值输出：");
        result2.forEach((k,value) -> {
            System.out.println(k + "---" + value);
        });
    }
    
    /**
     * 测试删除指定的列族
     */
    @Test
    public void testDeleteColumnFamily(){
        //添加测试数据
    	hBaseService.putData("test_base","777","f",new String[]{"var_name1"},new String[]{"555"});
    	hBaseService.putData("test_base","777","back",new String[]{"var_name2"},new String[]{"666"});
    	
        //取值
        Map<String,String> result1 = hBaseService.getRowData("test_base","777");
        System.out.println("第一次取值输出：");
        result1.forEach((k,value) -> {
            System.out.println(k + "---" + value);
        });
  
        //删除测试列族
        hBaseService.deleteColumnFamily("test_base","f");
 
        //再次取值
        Map<String,String> result2 = hBaseService.getRowData("test_base","777");
        System.out.println("第二次取值输出：");
        result2.forEach((k,value) -> {
            System.out.println(k + "---" + value);
        });
    }
    
    
}
