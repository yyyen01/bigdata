package lessons.bigdata.assignment2;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * Hello world!
 *
 */


public class HabaseExecutor 
{
    public static Logger logger = Logger.getLogger(HabaseExecutor.class);
    public static void main( String[] args ) throws IOException
    {
        
        
        String MY_NAMESPACE_NAME = "yen";
        String MY_TABLE = "student";
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        
        //create namespace
        if (!namespaceExists(admin, MY_NAMESPACE_NAME)) {
            logger.info("Creating Namespace [" + MY_NAMESPACE_NAME + "].");
            admin.createNamespace(NamespaceDescriptor
                .create(MY_NAMESPACE_NAME).build());
            logger.info("Namespace created successfully!");    
           }

           logger.info("Check if Namespace exists now:"+ namespaceExists(admin, MY_NAMESPACE_NAME));  
        
        TableName tableName = TableName.valueOf(MY_NAMESPACE_NAME+":"+MY_TABLE);
       
        String colFamily_info = "info";  
        String colFamily_score = "score";  

        // 建表
        if (admin.tableExists(tableName)) {
            logger.info("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily_info);
            hTableDescriptor.addFamily(hColumnDescriptor);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily_score);
            hTableDescriptor.addFamily(hColumnDescriptor2);
            admin.createTable(hTableDescriptor);
            logger.info("Table create successful");
        }

        String[][] data = {
            {"Tom","20210000000001","1","75","82"},
            {"Jerry","20210000000002","1","85","67"},
            {"Jack","20210000000003","2","80","80"},
            {"Rose","20210000000004","2","60","61"},
            {"YenYen","G20200343150210","1","70","90"}
        };

        logger.info("-----------------------------------------");
        logger.info("Insertion");
        logger.info("-----------------------------------------");
        // 插入数据
        Put put = null;
        for (String[] strings : data) {
            put = new Put(Bytes.toBytes(strings[0])); // row key
            put.addColumn(Bytes.toBytes(colFamily_info), Bytes.toBytes("student_id"), Bytes.toBytes(strings[1]));
            put.addColumn(Bytes.toBytes(colFamily_info), Bytes.toBytes("class"), Bytes.toBytes(strings[2])); 
            put.addColumn(Bytes.toBytes(colFamily_score), Bytes.toBytes("understanding"), Bytes.toBytes(strings[3])); 
            put.addColumn(Bytes.toBytes(colFamily_score), Bytes.toBytes("programming"), Bytes.toBytes(strings[4])); 
            conn.getTable(tableName).put(put);
            logger.info("Data insert success for rowKey:" + strings[0]);
        }
        
        scanTable(conn, tableName);

        logger.info("-----------------------------------------");
        logger.info("Delele Data");
        logger.info("-----------------------------------------");

        // 删除数据
        for (String[] rowKey : data) {
            Delete delete = new Delete(Bytes.toBytes(rowKey[0]));      // 指定rowKey
            conn.getTable(tableName).delete(delete);
            logger.info("Delete Success for rowkey : "+rowKey[0]);
        }

        scanTable(conn, tableName);

        logger.info("-----------------------------------------");
        logger.info("Delete Table");
        logger.info("-----------------------------------------");
        // // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            logger.info("Table Delete Successful");
        } else {
            logger.info("Table does not exist!");
        }
        logger.info("-----------------------------------------");

        logger.info("Check if table exist:"+admin.tableExists(tableName));

        //Delete namespace
        logger.info("-----------------------------------------");
        admin.deleteNamespace(MY_NAMESPACE_NAME);
        logger.info("Namaspace Deleted successfully.");
        logger.info("Check if Namaspace Exist:"+namespaceExists(admin,MY_NAMESPACE_NAME));

    
    }

    private static void scanTable(Connection conn, TableName tableName) throws IOException {
        
        logger.info("-----------------------------------------");
        logger.info("Scanning Table");
        logger.info("-----------------------------------------");
        Scan scan = new Scan();
        ResultScanner rsacn = conn.getTable(tableName).getScanner(scan);
        int count = 0;
        for(Result rs:rsacn) {

            String rowkey = Bytes.toString(rs.getRow());
            logger.info("row key :"+rowkey);
            Cell[] cells  = rs.rawCells();
            for(Cell cell : cells) {
               
                logger.info(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "."+
                Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())+"::"+
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                count++;
            }
            logger.info("-----------------------------------------");
        }

        if (count == 0){
            logger.info("No row found!");            
        }
                //closing the scanner
                rsacn.close();
                logger.info("-----------------------------------------");

    }

    //check if namespace Exists
    private static boolean namespaceExists(Admin admin,String name) throws IOException{    
        NamespaceDescriptor[] list = admin.listNamespaceDescriptors();
        logger.info("Current namespace : "+Arrays.toString(list));

        for (NamespaceDescriptor namespaceDescriptor : list) {
            if(namespaceDescriptor.getName().equals(name)){
                return true;
            }
        }
        return false;
    }
}

