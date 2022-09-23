package cn.lingkai5wu;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class MongoDBUtil {
    // 链接服务
    static MongoClient client = new MongoClient("localhost", 27017);
    // 链接库
    static MongoDatabase database = client.getDatabase("test");

    public static void main(String[] args) throws IOException {
        // CSV文件流
        // 使用相对路径，相对路径的起点为项目所在的路径
        File file = new File("data/2020410202.xlsx");
        xlsx2Mongo(file);
    }

    public static void xlsx2Mongo(@NotNull File file) throws IOException {
        // 获取文件名
        String name = file.getName();
        // 定义集合名
        String id = name.substring(0, name.lastIndexOf("."));
        // 链接集合
        MongoCollection col = database.getCollection("DB" + id);
        //　drop，便于测试
        col.drop();
        // 输出开始信息
        System.out.print("正在处理: " + name);
        long start = System.currentTimeMillis();

        // 输入流
        InputStream fis = new FileInputStream(file);
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
                if (cur.getCellType() == 0 && !DateUtil.isCellDateFormatted(cur)) {
                    // 为数值，转换为double加入到Document中
                    curDocument.append(keys[j], Double.valueOf(s));
                } else {
                    // 为字符串，转为字符串加入
                    curDocument.append(keys[j], s);
                }
            }
            // 将当前行构建的Document添加到List中
            list.add(curDocument);
        }
        // 将List中所有Document全部加入到数据库
        col.insertMany(list);
        System.out.println("\t|\t完成，耗时 " + (System.currentTimeMillis() - start) + "ms");
    }
}
