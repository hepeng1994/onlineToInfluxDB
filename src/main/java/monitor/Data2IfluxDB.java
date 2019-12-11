package monitor;


import monitorutil.InfluxDBConnect;
import monitorutil.PropertiesUtil;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;
import monitorutil.InfluxDBConnection;
import monitorhandler.Transfrom;

import java.io.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.stream.Collectors;

/**
 * 功能描述: <br>
 * 〈在线检测数据写入influxDB〉
 *
 * @Author: 何鹏
 * @Date: 2019/10/15 13:52
 */

public class Data2IfluxDB {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    static PropertiesUtil propertiesUtil;

    static {
        try {
            propertiesUtil = new PropertiesUtil();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //存储上一个文件每个机器每个通道的时间
    static Map lastTime = new HashMap<String, Long>();
    //static Connection c = null;
    static ArrayList<String> remove = new ArrayList<String>();

    static {
        remove.add("onlinemonitordata_1_sirui_bearing_current");
        remove.add("onlinemonitordata_1_sirui_front_rotor_vibration");
        remove.add("onlinemonitordata_1_sirui_insulation_overheating");
        remove.add("onlinemonitordata_1_sirui_rear_rotor_vibration");
        // remove.add("onlinemonitordata_1_sirui_partial_discharge");
        remove.add("onlinemonitordata_2_sirui_bearing_current");
        remove.add("onlinemonitordata_2_sirui_front_rotor_vibration");
        remove.add("onlinemonitordata_2_sirui_insulation_overheating");
        remove.add("onlinemonitordata_2_sirui_rear_rotor_vibration");
        //remove.add("onlinemonitordata_2_sirui_partial_discharge");
    }
   /* static {
        try {

            String pgsql_url = propertiesUtil.readValue("pgsql_url");
            System.out.println(pgsql_url);
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    //.getConnection("jdbc:postgresql://localhost:5432/postgres",
                    .getConnection(pgsql_url,
                            //.getConnection("jdbc:postgresql://172.17.195.191:5432/postgres",
                            "postgres", "postgres");
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        try {
            Statement stmt = c.createStatement();
            String sql = "";
            //控制机器号
            for (int i = 1; i <= 2; i++) {
                //控制通道号
                for (int j = 1; j <= 3; j++) {
                    sql = String.format(" SELECT * FROM partial_discharge  where machine='%s' AND channelId=%s ORDER BY createtime DESC LIMIT 1", i, j);
                    System.out.println(sql);
                    ResultSet resultSet = stmt.executeQuery(sql);
                    while (resultSet.next()) {
                        Date createtime = sdft.parse(resultSet.getString("createtime"));
                        String key = resultSet.getString("machine") + resultSet.getInt("channelId");
                        lastTime.put(key, createtime.getTime());
                        System.out.println(i + "号机" + j + "通道key:" + key + " value:" + createtime.getTime());

                    }
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }*/

    public Data2IfluxDB() throws IOException {
    }


    public static void main(String[] args) throws InterruptedException, SQLException, ParseException, IOException {
        String influx_db = propertiesUtil.readValue("influx_db");
        String influx_url = propertiesUtil.readValue("influx_url");
        InfluxDBConnect influxDBConnection = new InfluxDBConnect("admin", "admin", influx_url, influx_db);
        //ReadInfluxDB(influxDBConnection);

        while (true) {
            Thread.sleep(5000);
            String file_path = propertiesUtil.readValue("file");
            File file = new File(file_path);
            List<String> collect = Arrays.stream(file.listFiles()).map(File::getName).collect(Collectors.toList());
            collect.retainAll(remove);
            if (collect.size() > 0) {
                file2TD(influxDBConnection, file);

            }
        }

    }

    /**
     * 功能描述: <br>
     * 〈获取微妙〉
     *
     * @Param: []
     * @Return: []
     * @Author: 何鹏
     * @Date: 2019/10/21 15:15
     */
    public static Long getmicTime() {
        Long cutime = System.currentTimeMillis() * 1000; // 微秒
        Long nanoTime = System.nanoTime(); // 纳秒
        return cutime + (nanoTime - nanoTime / 1000000 * 1000000) / 1000;
    }


    private static void file2TD(InfluxDBConnect influxDBConnection, File file1) throws SQLException, ParseException {


        File[] files = file1.listFiles();

        //遍历
        for (int i = 0; i < files.length; i++) {
            String machine = files[i].getName();

            //振动入库
            if (machine.equals("onlinemonitordata_1_sirui_rear_rotor_vibration")) {

                rotor_vibration(influxDBConnection, files[i]);

            }
            if (machine.equals("onlinemonitordata_2_sirui_rear_rotor_vibration")) {

                rotor_vibration(influxDBConnection, files[i]);

            }
            if (machine.equals("onlinemonitordata_2_sirui_front_rotor_vibration")) {

                rotor_vibration(influxDBConnection, files[i]);

            }
            if (machine.equals("onlinemonitordata_1_sirui_front_rotor_vibration")) {

                rotor_vibration(influxDBConnection, files[i]);

            }
            //绝缘过热
            if (machine.equals("onlinemonitordata_2_sirui_insulation_overheating")) {
                insulation_overheating(influxDBConnection, files[i]);

            }
            if (machine.equals("onlinemonitordata_1_sirui_insulation_overheating")) {
                insulation_overheating(influxDBConnection, files[i]);

            }
            //轴电流
            if (machine.equals("onlinemonitordata_2_sirui_bearing_current")) {
                bearing_current(influxDBConnection, files[i]);

            }
            if (machine.equals("onlinemonitordata_1_sirui_bearing_current")) {
                bearing_current(influxDBConnection, files[i]);

            }
          /*  if (machine.startsWith("onlinemonitordata_2_sirui_partial_discharge")) {
                partial_discharge(c, files[i], "partial_discharge", "2");
            }
            if (machine.startsWith("onlinemonitordata_1_sirui_partial_discharge")) {
                partial_discharge(c, files[i], "partial_discharge", "1");
            }*/
        }

    /*    for (int i = 0; i < files.length; i++) {
            files[i].delete();
        }*/
    }

    /**
     * 功能描述: <br>
     * 〈读文件〉
     *
     * @Param: [fileName]
     * @Return: [fileName]
     * @Author: 何鹏
     * @Date: 2019/10/16 16:31
     */
    public static String readToString(String fileName) {
        String encoding = "UTF-8";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(filecontent);
            in.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
            System.err.println("The OS does not support " + encoding);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 功能描述: <br>
     * 〈振动数据解析入库〉
     *
     * @Param: [influxDBConnection, file, tableName, subTypeId]
     * @Return: [influxDBConnection, file, tableName, subTypeId]
     * @Author: 何鹏
     * @Date: 2019/10/15 13:54
     */
    public static void rotor_vibration(InfluxDBConnect influxDBConnection, File file) {
        //上一个文件时间
        Long lastFileTime = (Long) lastTime.getOrDefault(file.getName(), 0L);
        //本次文件传入时间
        long fileTime = file.lastModified();
        if (fileTime > lastFileTime) {
            long startTime = System.currentTimeMillis();
            List<String> records = new ArrayList<String>();
            //读取文件
            String line = readToString(file.toString());
            //切分数据
            String[] split = line.split("\n");
            for (int i1 = 1; i1 <= split.length; i1++) {
                try {
                    String[] split2 = split[i1 - 1].split(" ");
                    //拼接数据时间
                    String date = split2[0] + " " + split2[1];
                    //将数据分为4通道
                    ArrayList arrayList1 = Transfrom.Transfrom_shake(split2[2]);
                    //剔除脏数据
                    Long dataTime = sdf.parse(date).getTime();
                    long nowTime = System.currentTimeMillis();
                    long timeDifference = nowTime - dataTime;
                    if (dataTime <= nowTime & timeDifference < 86400000) {

                        //封装数据准备入库influxdb
                        Map<String, String> tags = new HashMap<String, String>();
                        Map<String, Object> fields = new HashMap<String, Object>();

                        fields.put("sensor1", arrayList1.get(0));
                        fields.put("sensor2", arrayList1.get(1));
                        fields.put("sensor3", arrayList1.get(2));
                        fields.put("sensor4", arrayList1.get(3));
                        //震动解析数据剩余(解析未知)
                        fields.put("sensor5", arrayList1.get(4));
                        Point point = influxDBConnection.pointBuilder(file.getName().replace("sirui", "tsziot"), dataTime, tags, fields);
                        BatchPoints batchPoints1 = BatchPoints.database("xiangtan")
                                .retentionPolicy("").consistency(InfluxDB.ConsistencyLevel.ALL).build();
                        batchPoints1.point(point);

                        records.add(batchPoints1.lineProtocol());
                    } else {
                        System.out.println("当前时间:" + sdf.format(new Date(nowTime)) + "本次数据时间为:" + date);
                        continue;
                    }
                } catch (Exception e) {
                    //System.out.println(split[i1 - 1].toString());
                }
            }
            //将数据入库influxdb
            influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
            //将当前文件时间写入map
            lastTime.put(file.getName(), fileTime);
            long stopTime = System.currentTimeMillis();
            Date date = new Date();
            String date_format = sdf.format(date);
            System.out.println("时间:" + date_format + "," + file.getName() + "文件夹所用时间:" + (stopTime - startTime) + "毫秒");
        }
        //file.delete();
    }

    /**
     * 功能描述: <br>
     * 〈绝缘过热数据解析入库〉
     *
     * @Param: [influxDBConnection, file, tableName]
     * @Return: [influxDBConnection, file, tableName]
     * @Author: 何鹏
     * @Date: 2019/10/15 13:54
     */
    private static void insulation_overheating(InfluxDBConnect influxDBConnection, File file) {
        //上一个文件时间
        Long lastFileTime = (Long) lastTime.getOrDefault(file.getName(), 0L);
        //本次文件传入时间
        long fileTime = file.lastModified();
        if (fileTime > lastFileTime) {
            long startTime = System.currentTimeMillis();
            //创建入库所需list
            List<String> records = new ArrayList<String>();
            //读取数据
            String line = readToString(file.toString());
            String[] split = line.split("\n");


            for (int i1 = 1; i1 <= split.length; i1++) {
                try {
                    String[] split2 = split[i1 - 1].split(" ");
                    //拼接时间
                    String date = split2[0] + " " + split2[1];
                    //将数据切分为4通道
                    ArrayList arrayList1 = Transfrom.Transfrom_hot(split2[2]);

                    Long dataTime = 0L;
                    try {
                        dataTime = sdf.parse(date).getTime();
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }

                    long nowTime = System.currentTimeMillis();
                    long timeDifference = nowTime - dataTime;
                    //剔除脏数据
                    if (dataTime <= nowTime & timeDifference < 86400000) {
                        //封装数据,准备入库influxDB
                        Map<String, String> tags = new HashMap<String, String>();
                        Map<String, Object> fields = new HashMap<String, Object>();

                        fields.put("sensor1", arrayList1.get(0));
                        fields.put("sensor2", arrayList1.get(1));
                        fields.put("sensor3", arrayList1.get(2));

                        // influxDBConnection.insert("rear_rotor_vibration_1",tags,fields,time,TimeUnit.MILLISECONDS);

                        Point point = influxDBConnection.pointBuilder(file.getName().replace("sirui", "tsziot"), dataTime, tags, fields);
                        BatchPoints batchPoints1 = BatchPoints.database("xiangtan")
                                .retentionPolicy("").consistency(InfluxDB.ConsistencyLevel.ALL).build();
                        batchPoints1.point(point);

                        records.add(batchPoints1.lineProtocol());


                    } else {
                        System.out.println("当前时间:" + sdf.format(new Date(nowTime)) + "本次数据时间为:" + date);
                        continue;
                    }
                } catch (Exception e) {
                    //System.out.println(split[i1 - 1].toString());
                }
            }
            //入库influxDB
            influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
            //将本次数据时间写入map
            lastTime.put(file.getName(), fileTime);
            long stopTime = System.currentTimeMillis();
            Date date = new Date();
            String date_format = sdf.format(date);
            System.out.println("时间:" + date_format + "," + file.getName() + "文件夹所用时间:" + (stopTime - startTime) + "毫秒");
        }
        // file.delete();

    }

    /**
     * 功能描述: <br>
     * 〈轴电流数据解析入库〉
     *
     * @Param: [influxDBConnection, file, tableName]
     * @Return: [influxDBConnection, file, tableName]
     * @Author: 何鹏
     * @Date: 2019/10/15 13:55
     */
    private static void bearing_current(InfluxDBConnect influxDBConnection, File file) {
        //上一个文件时间
        Long lastFileTime = (Long) lastTime.getOrDefault(file.getName(), 0L);
        //本次文件传入时间
        long fileTime = file.lastModified();
        //判断文件有没有更新
        if (fileTime > lastFileTime) {
            long startTime = System.currentTimeMillis();

            List<String> records = new ArrayList<String>();
            //读取数据
            String line = readToString(file.toString());
            String[] split = line.split("\n");

            for (int i1 = 1; i1 <= split.length; i1++) {
                try {
                    String[] split2 = split[i1 - 1].split(" ");
                    //拼接数据时间
                    String date = split2[0] + " " + split2[1];
                    Long dataTime = 0L;
                    try {
                        dataTime = sdf.parse(date).getTime();
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    long nowTime = System.currentTimeMillis();
                    long timeDifference = nowTime - dataTime;
                    //剔除脏数据
                    if (dataTime <= nowTime & timeDifference < 86400000) {
                        //封装数据准备入库influxDB
                        Map<String, String> tags = new HashMap<String, String>();
                        Map<String, Object> fields = new HashMap<String, Object>();

                        fields.put("sensor1", split2[2]);

                        Point point = influxDBConnection.pointBuilder(file.getName().replace("sirui", "tsziot"), dataTime, tags, fields);
                        BatchPoints batchPoints1 = BatchPoints.database("xiangtan")
                                .retentionPolicy("").consistency(InfluxDB.ConsistencyLevel.ALL).build();
                        batchPoints1.point(point);

                        records.add(batchPoints1.lineProtocol());

                    } else {
                        //打印脏数据时间戳
                        System.out.println("当前时间:" + sdf.format(new Date(nowTime)) + "本次数据时间为:" + date);
                        continue;
                    }

                } catch (Exception e) {
                    // System.out.println(split[i1 - 1].toString());
                }
            }
            //入库influxDB
            influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
            //将本次时间写入map
            lastTime.put(file.getName(), fileTime);
            long stopTime = System.currentTimeMillis();
            Date date = new Date();
            String date_format = sdf.format(date);
            System.out.println("时间:"+date_format +","+ file.getName()+"文件夹所用时间:" + (stopTime - startTime) + "毫秒");
        }
        //file.delete();

    }

    /**
     * 功能描述: <br>
     * 〈局部放电写入postgressql〉
     *
     * @Param: [c, file, tableName, machine]
     * @Return: [c, file, tableName, machine]
     * @Author: 何鹏
     * @Date: 2019/10/16 16:32
     */
   /* private static void partial_discharge(Connection c, File file, String tableName, String machine) throws SQLException, ParseException {
        String s;
        s = readToString(file.toString());
        String[] value = s.split("\n");
        Statement stmt = c.createStatement();
        for (int i = 0; i < value.length; i++) {
            String[] split = value[i].split(" ");
            Date parse = null;
            String format = "";
            parse = sdf.parse(split[0] + " " + split[1]);
            long time = parse.getTime();
            if ((long) lastTime.getOrDefault(machine + split[2], 0L) < time) {
                lastTime.put(machine + split[2], time);
                format = sdf.format(parse);
                String sql = String.format("insert into %s (createtime,machine,channelId,pdi,pdiminus,pdiplus,q02,q02minus,q02mv,q02plus,sumpdampl,sumpdminus,sumpdplus,pdexists,humidity,temperature) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tableName, "'" + format + "'", machine, split[2], split[3], split[4], split[5], split[6], split[7], split[8], split[9], split[10], split[11], split[12], split[13], split[14], split[15]);
                stmt.executeUpdate(sql);
            }
        }
        stmt.close();
        c.commit();
        file.delete();
    }
*/
    private static void ReadInfluxDB(InfluxDBConnection influxDBConnection) {

        QueryResult query = influxDBConnection.query("select count(sersor1) from onlinemonitordata_1_tsziot_front_rotor_vibration");
        //QueryResult query = influxDBConnection.query("select * from insulation_overheating_1 ORDER BY time DESC limit 1 tz('Asia/Shanghai')");
        QueryResult.Result result = query.getResults().get(0);
        if (result.getSeries() != null) {
            List<List<Object>> valueList = result.getSeries().stream().map(QueryResult.Series::getValues)
                    .collect(Collectors.toList()).get(0);
            if (valueList != null && valueList.size() > 0) {
                for (List<Object> value : valueList) {
                    Map<String, String> map = new HashMap<String, String>();
                    // 数据库中字段1取值
                    String field1 = value.get(0) == null ? null : value.get(0).toString();
                    // 数据库中字段2取值
                    String field2 = value.get(1) == null ? null : value.get(1).toString();

                    // 数据库中字段3取值
                    //String field3 = value.get(2) == null ? null : value.get(2).toString();
                    // 数据库中字段4取值
                    //  String field4 = value.get(3) == null ? null : value.get(3).toString();
                  /*  // 数据库中字段5取值
                    String field5 = value.get(4) == null ? null : value.get(4).toString();*/
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    System.out.println("time:" + field1);
                    System.out.println("sersor1:" + field2);
                    //System.out.println("serson2:" + field3);
                    //System.out.println("serson3:" + field4);
                    // System.out.println("serson4:" + field5);
                }
            }
        }
    }
}
