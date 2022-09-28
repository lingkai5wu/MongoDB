package cn.lingkai5wu;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

public class Table2MongoDB {
    // 链接服务
    static MongoClient client = new MongoClient("localhost", 27017);
    // 链接库
    static MongoDatabase database = client.getDatabase("test");

    public static void main(String[] args) throws Exception {
        // 使用相对路径，相对路径的起点为项目所在的路径
        // CSV文件流 加入文件夹
        File file = new File("data");
        // drop整个库
        database.drop();
        // 遍历文件夹内的文件
        for (File f : Objects.requireNonNull(file.listFiles())) {
            // 记录开始时间
            long start = System.currentTimeMillis();
            // 调用执行
            if (f.getName().endsWith("csv")) {
                csv2Mongo(f);
            } else {
                xlsx2Mongo(f);
            }
            // 输出耗时
            System.out.println(f.getName() + "\t" + (System.currentTimeMillis() - start) + "ms");
        }
    }

    // 处理xlsx文件
    public static void xlsx2Mongo(@NotNull File file) throws IOException {
        // 获取连接
        MongoCollection<Document> col = getCol(file);

        // 字节流
        InputStream fis = Files.newInputStream(file.toPath());
        // 读取整个Excel
        XSSFWorkbook sheets = new XSSFWorkbook(fis);
        // 获取第一个Sheet
        XSSFSheet sheet = sheets.getSheetAt(0);

        // 获取首行
        XSSFRow title = sheet.getRow(0);
        // 获取首行单元格个数，即非关系数据库的字段数
        int colNum = title.getPhysicalNumberOfCells();
        // 定义数组存储字段
        String[] keys = new String[colNum];
        for (int i = 0; i < colNum; i++) {
            //　加入到数组
            keys[i] = String.valueOf(title.getCell(i));
        }

        // 获取单元格行数
        int rowNum = sheet.getPhysicalNumberOfRows();
        // 定义List用来存放每行内容（给定大小，减少扩容带来的时间），用于后续insertMany()
        List<Document> list = new ArrayList<>(rowNum);
        // 从第1行开始遍历（第0行是表头）
        for (int i = 1; i < rowNum; i++) {
            // 构建Document
            Document curDocument = new Document();
            // 获取当前行内容
            XSSFRow curRow = sheet.getRow(i);
            // 循环本行的每个单元格
            for (int j = 0; j < colNum; j++) {
                // 获取单元格内容
                XSSFCell cur = curRow.getCell(j);
                // 转换为字符串
                String s = cur.toString();
                // 判断是否为数值类型，数值类型为0，且需要对日期进行特判（日期会被判断为数值，可恶）
                // 尝试不使用IPO
//                if (cur.getCellType() == 0 && !DateUtil.isCellDateFormatted(cur)) {
//                    // 为数值，转换为double加入到Document中
//                    curDocument.append(keys[j], Double.valueOf(s));
//                } else {
//                    // 为字符串，转为字符串加入
//                    curDocument.append(keys[j], s);
//                }
                if (isNumeric(s)) {
                    curDocument.append(keys[j], Double.valueOf(s));
                } else {
                    curDocument.append(keys[j], s);
                }
            }
            // 将当前行构建的Document添加到List中
            list.add(curDocument);
        }
        // 将List中所有Document全部加入到数据库
        col.insertMany(list);
    }

    // 处理csv文件
    public static void csv2Mongo(@NotNull File file) throws Exception {
        // 获取链接
        MongoCollection<Document> col = getCol(file);
        // 字节流
        FileInputStream fin = new FileInputStream(file);
        // 字符流
        Reader reader = new InputStreamReader(fin, codeString(file));
        // 表头 即字段名
        String[] keys;
        // csv的Reader
        CSVReader csvReader = new CSVReader(reader);
        // 定义容器
        List<String[]> list;
        try {
            // 读入所需表头和数据区域内容
            keys = csvReader.readNext();
            list = csvReader.readAll();
        } catch (CsvException e) {
            throw new RuntimeException(e);
        }

        // 字段数
        int n = keys.length;
        // 构造Document的容器
        List<Document> documents = new ArrayList<>(list.size());
        for (String[] ss : list) {
            // 构造当前Document
            Document cur = new Document();
            for (int i = 0; i < n; i++) {
                // 判断是否为数值类型，数值类型使用Double，非数值使用String，空则跳过
                if (!ss[i].isEmpty()) {
                    cur.append(keys[i], isNumeric(ss[i]) ? Double.valueOf(ss[i]) : ss[i]);
                }
            }
            // 加入到容器中
            documents.add(cur);
        }
        // 全部插入到数据库
        col.insertMany(documents);
    }

    // 获取链接
    private static MongoCollection<Document> getCol(@NotNull File file) {
        // 获取文件名
        String name = file.getName();
        // 定义集合名
        String id = name.substring(0, name.lastIndexOf("."));
        // 链接集合
        MongoCollection<Document> col = database.getCollection("NO" + id);
        //　drop，便于测试
        col.drop();
        return col;
    }

    private static final Pattern NUMBER_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");

    // 正则表达式判断是否为数字 cp:https://blog.csdn.net/mryang125/article/details/113146057
    public static boolean isNumeric(String str) {
        return str != null && NUMBER_PATTERN.matcher(str).matches();
    }

    // 简单判断编码 cp:https://blog.csdn.net/m0_48983233/article/details/122893008
    public static String codeString(@NotNull File file) throws Exception {
        BufferedInputStream bin = new BufferedInputStream(Files.newInputStream(file.toPath()));
        int p = (bin.read() << 8) + bin.read();
        bin.close();
        String code;
        switch (p) {
            case 0xefbb:
                code = "UTF-8";
                break;
            case 0xfffe:
                code = "Unicode";
                break;
            case 0xfeff:
                code = "UTF-16BE";
                break;
            default:
                code = "GBK";
        }
        return code;
    }
}
