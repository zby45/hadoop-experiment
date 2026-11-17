import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 实验三 HBase Java API 实现
 * 实现题目要求的5个核心方法：createTable / addRecord / scanColumn / modifyData / deleteRow
 */
public class HBaseExperiment {

    private static Connection connection;
    private static Admin admin;

    // 初始化HBase连接
    public static void initConnection() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();
    }

    /**
     * 1. 创建表：如果表已存在则先删除，再按字段数组创建列族
     * @param tableName 表名
     * @param fields    列族名称数组
     */
    public static void createTable(String tableName, String[] fields) throws IOException {
        TableName tName = TableName.valueOf(tableName);
        // 表存在则先删除
        if (admin.tableExists(tName)) {
            admin.disableTable(tName);
            admin.deleteTable(tName);
            System.out.println("已删除原有表：" + tableName);
        }
        // 构造表描述器，添加列族
        TableDescriptorBuilder descBuilder = TableDescriptorBuilder.newBuilder(tName);
        for (String field : fields) {
            ColumnFamilyDescriptor family = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(field)).build();
            descBuilder.setColumnFamily(family);
        }
        admin.createTable(descBuilder.build());
        System.out.println("创建表成功：" + tableName + "，列族：" + String.join(",", fields));
    }

    /**
     * 2. 向表指定行添加数据，fields格式为 列族:列限定符
     * @param tableName 表名
     * @param row       rowkey（学生姓名）
     * @param fields    字段数组，格式 family:qualifier
     * @param values    对应字段的值
     */
    public static void addRecord(String tableName, String row, String[] fields, String[] values) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < fields.length; i++) {
            String[] col = fields[i].split(":");
            String family = col[0];
            String qualifier = col.length > 1 ? col[1] : "";
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(values[i]));
        }
        table.put(put);
        table.close();
        System.out.println("向表 " + tableName + " 插入行 " + row + " 成功");
    }

    /**
     * 3. 浏览表某一列数据：传入列族名则输出该列族下所有列；传入family:qualifier则只输出指定列
     * @param tableName 表名
     * @param column    列族名 或 family:qualifier
     */
    public static void scanColumn(String tableName, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String[] col = column.split(":");
        String family = col[0];
        if (col.length > 1) {
            // 指定具体列
            scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(col[1]));
        } else {
            // 指定列族，输出该列族下所有列
            scan.addFamily(Bytes.toBytes(family));
        }
        ResultScanner scanner = table.getScanner(scan);
        System.out.println("===== 扫描表 " + tableName + " 的列 " + column + " =====");
        for (Result result : scanner) {
            String rowKey = Bytes.toString(result.getRow());
            List<Cell> cells = result.listCells();
            if (cells == null || cells.isEmpty()) {
                System.out.println("行 " + rowKey + " 该列数据不存在：null");
                continue;
            }
            for (Cell cell : cells) {
                String q = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("行:" + rowKey + "  列:" + family + ":" + q + "  值:" + value);
            }
        }
        scanner.close();
        table.close();
    }

    /**
     * 4. 修改指定行、指定列单元格的数据
     * @param tableName 表名
     * @param row       rowkey
     * @param column    列，格式 family:qualifier
     * @param newValue  新值
     */
    public static void modifyData(String tableName, String row, String column, String newValue) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        String[] col = column.split(":");
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(col[0]), Bytes.toBytes(col[1]), Bytes.toBytes(newValue));
        table.put(put);
        table.close();
        System.out.println("修改表 " + tableName + " 行 " + row + " 的 " + column + " 为：" + newValue);
    }

    /**
     * 5. 删除表中指定行的全部记录
     * @param tableName 表名
     * @param row       rowkey
     */
    public static void deleteRow(String tableName, String row) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(row));
        table.delete(delete);
        table.close();
        System.out.println("已删除表 " + tableName + " 中的行：" + row);
    }

    public static void main(String[] args) throws IOException {
        initConnection();

        // ========== 1. 创建三张表 ==========
        // 学生表：列族info
        createTable("student", new String[]{"info"});
        // 课程表：列族info
        createTable("course", new String[]{"info"});
        // 选课表：列族score
        createTable("sc", new String[]{"score"});

        // ========== 2. 插入学生表数据 ==========
        addRecord("student", "Zhangsan",
                new String[]{"info:S_No", "info:S_Sex", "info:S_Age"},
                new String[]{"2015001", "male", "23"});
        addRecord("student", "Mary",
                new String[]{"info:S_No", "info:S_Sex", "info:S_Age"},
                new String[]{"2015003", "female", "22"});
        addRecord("student", "Lisi",
                new String[]{"info:S_No", "info:S_Sex", "info:S_Age"},
                new String[]{"2015003", "male", "24"});

        // ========== 3. 插入课程表数据 ==========
        addRecord("course", "Math",
                new String[]{"info:C_No", "info:C_Credit"},
                new String[]{"123001", "2.0"});
        addRecord("course", "Computer Science",
                new String[]{"info:C_No", "info:C_Credit"},
                new String[]{"123002", "5.0"});
        addRecord("course", "English",
                new String[]{"info:C_No", "info:C_Credit"},
                new String[]{"123003", "3.0"});

        // ========== 4. 插入选课表数据 ==========
        addRecord("sc", "2015001:123001",
                new String[]{"score:Math"},
                new String[]{"86"});
        addRecord("sc", "2015001:123003",
                new String[]{"score:English"},
                new String[]{"69"});
        addRecord("sc", "2015002:123002",
                new String[]{"score:Computer Science"},
                new String[]{"77"});
        addRecord("sc", "2015002:123003",
                new String[]{"score:English"},
                new String[]{"99"});
        addRecord("sc", "2015003:123001",
                new String[]{"score:Math"},
                new String[]{"98"});
        addRecord("sc", "2015003:123002",
                new String[]{"score:Computer Science"},
                new String[]{"95"});

        // ========== 5. 测试API方法 ==========
        // 扫描学生表性别列
        scanColumn("student", "info:S_Sex");
        // 修改Lisi的年龄为25
        modifyData("student", "Lisi", "info:S_Age", "25");
        // 重新扫描年龄列验证修改结果
        scanColumn("student", "info:S_Age");
        // 删除Mary这一行
        deleteRow("student", "Mary");
        // 再次扫描验证删除结果
        scanColumn("student", "info");

        // 关闭资源
        admin.close();
        connection.close();
        System.out.println("HBase API 实验执行完成");
    }
}
