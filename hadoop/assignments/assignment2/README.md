# Assignment2 

### Requirement
#### Using Java API to implement create, insert, delete, search function of HBase.
<br>

## Java Implementation
#### Refer to HabaseExecutor.java
[HabaseExecutor.java](src/main/java/lessons/bigdata/assignment2/HabaseExecutor.java)

### Explanation on Implementation 
#### 1. Create connection 
       
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();


#### 2. Create namespace and check namespace
    if (!namespaceExists(admin, MY_NAMESPACE_NAME)) {
            logger.info("Creating Namespace [" + MY_NAMESPACE_NAME + "].");
            admin.createNamespace(NamespaceDescriptor
                .create(MY_NAMESPACE_NAME).build());
            logger.info("Namespace created successfully!");    
           }

           logger.info("Check if Namespace exists now:"+ namespaceExists(admin, MY_NAMESPACE_NAME));  

#### 3. Create table
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

#### 4. Load Data
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

#### 5. Scan table and print all data in the table
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

#### 6. Delete Data
         for (String[] rowKey : data) {
            Delete delete = new Delete(Bytes.toBytes(rowKey[0]));      // 指定rowKey
            conn.getTable(tableName).delete(delete);
            logger.info("Delete Success for rowkey : "+rowKey[0]);
        }        

#### 7. Delete Table
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            logger.info("Table Delete Successful");
        } else {
            logger.info("Table does not exist!");
        }

#### 8. Delete Namespace
        logger.info("-----------------------------------------");
        admin.deleteNamespace(MY_NAMESPACE_NAME);
        logger.info("Namaspace Deleted successfully.");
        logger.info("Check if Namaspace Exist:"+namespaceExists(admin,MY_NAMESPACE_NAME));


### Exection log
#### Refer to this link 
[Execution log](logs/execution.log)