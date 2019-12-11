package monitor;

import monitorutil.InfluxDBConnect;
import monitorutil.PgsqlConnect;
import monitorutil.PgsqlDataSource;
import monitorutil.PropertiesUtil;
import monitorhandler.Transfrom;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;


import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

public class Data2InfluxDB_History {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    static SimpleDateFormat sdft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static PropertiesUtil propertiesUtil;

    static {
        try {
            propertiesUtil = new PropertiesUtil();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //存储上一个文件每个机器每个通道的时间
    static Map lastTime = new HashMap<String, String>();
    static Connection c = null;
    static ArrayList<String> remove =new ArrayList<String>();
    static {
        remove.add("onlinemonitordata_1_sirui_bearing_current");
        remove.add("onlinemonitordata_1_sirui_front_rotor_vibration");
        remove.add("onlinemonitordata_1_sirui_insulation_overheating");
        remove.add("onlinemonitordata_1_sirui_rear_rotor_vibration");
         remove.add("onlinemonitordata_1_sirui_partial_discharge");
        remove.add("onlinemonitordata_2_sirui_bearing_current");
        remove.add("onlinemonitordata_2_sirui_front_rotor_vibration");
        remove.add("onlinemonitordata_2_sirui_insulation_overheating");
        remove.add("onlinemonitordata_2_sirui_rear_rotor_vibration");
        remove.add("onlinemonitordata_2_sirui_partial_discharge");
    }
    static {
        try {
              PgsqlDataSource pgsqlDataSource = new PgsqlDataSource();
              c = pgsqlDataSource.getConnection();
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
            for (int i = 1; i <= 3; i++) {
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
    }




    public static void main(String[] args) throws InterruptedException, SQLException, ParseException, IOException {
        String arg = args[0];
        System.out.println(arg);
        String influx_db = propertiesUtil.readValue("influx_db");
        String influx_url = "http://192.168.31.210:8086";
        //String influx_url = "http://47.94.128.225:8086";
       InfluxDBConnect influxDBConnection = new InfluxDBConnect("admin", "admin", influx_url, influx_db);
       /*  long startTime = System.currentTimeMillis();
        ReadInfluxDB(influxDBConnection);
        long stopTime = System.currentTimeMillis();
        System.out.println("文件夹所用时间:" + (stopTime - startTime) + "毫秒");*/
        //File file_History = new File("F:\\data\\test\\无重复");
        File file_History = new File(arg);
        File[] files = file_History.listFiles();
        for (int i = 0; i < files.length; i++) {
            System.out.println("进入for循环");
            Thread.sleep(2000);
            //System.out.println(file_path);
            // File file = new File("/root/1561101780000");
            File file = new File(files[i].toString());
            List<String> collect = Arrays.stream(file.listFiles()).map(File::getName).collect(Collectors.toList());
            collect.retainAll(remove);
            if (collect.size() >1) {
                long startTime = System.currentTimeMillis();
                file2TD(influxDBConnection,file);
                long stopTime = System.currentTimeMillis();
                Date date = new Date();
                String date_format = sdf.format(date);
                System.out.println(date_format+"文件夹所用时间:" + (stopTime - startTime) + "毫秒");
            }

        }


    }
    /**
     * 功能描述: <br>
     * 〈获取微妙〉
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


    private static void file2TD(InfluxDBConnect influxDBConnection, File file1

    ) throws SQLException, ParseException  {


        File[] files = file1.listFiles();

        //遍历
        for (int i = 0; i < files.length; i++) {
            String machine = files[i].getName();
            // String machine = files[i].toString().split("/")[files[i].toString().split("/").length - 1];
            //String machine = files[i].toString().split("\\\\")[files[i].toString().split("\\\\").length - 1];

            //振动入库
            if (machine.endsWith("rotor_vibration")) {

                rotor_vibration(influxDBConnection, files[i]);

            }

            //绝缘过热
            if (machine.endsWith("insulation_overheating")) {
                insulation_overheating(influxDBConnection, files[i]);

            }
            //轴电流
            if (machine.endsWith("bearing_current")) {
                bearing_current(influxDBConnection, files[i]);

            }

            if (machine.endsWith("partial_discharge")) {
                partial_discharge(c, files[i]);
            }

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
    private static void rotor_vibration(InfluxDBConnect influxDBConnection, File file) {


        String s;
        List<String> records = new ArrayList<String>();
        //读取文件
        s = readToString(file.toString());
        String[] split = s.split("\n");
        String tableName = "tdm" + file.getName().replace("onlinemonitordata_", "").replace("_sirui", "");
        for (int i1 = 1; i1 <= split.length; i1++) {
        try {
            String[] split2 = split[i1 - 1].split(" ");
            String date = split2[0] + " " + split2[1];
            ArrayList arrayList1 = Transfrom.Transfrom_shake(split2[2]);
            Long time = 0L;
            try {
                time = sdf.parse(date).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }


            Map<String, String> tags = new HashMap<String, String>();
            Map<String, Object> fields = new HashMap<String, Object>();
            fields.put("non_outgoing_x", arrayList1.get(0));
            fields.put("non_outgoing_y", arrayList1.get(1));
            fields.put("outgoing_x", arrayList1.get(2));
            fields.put("outgoing_y", arrayList1.get(3));
            //震动解析数据剩余(解析未知)
            //fields.put("sensor5", arrayList1.get(4));


            Point point = influxDBConnection.pointBuilder(tableName, time, tags, fields);
            BatchPoints batchPoints1 = BatchPoints.database("xiangtan")
                    .retentionPolicy("").consistency(InfluxDB.ConsistencyLevel.ALL).build();
            batchPoints1.point(point);

            records.add(batchPoints1.lineProtocol());

        }catch (Exception e){
            System.out.println(file.getPath());
            System.out.println(split[i1 - 1]);
        }
        }

        // long startTime = System.currentTimeMillis();
        try {

        influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
        file.delete();
        }catch (Exception e){
            System.out.println("振动入库失败");
            e.printStackTrace();
        }
        // long stopTime = System.currentTimeMillis();
        // System.out.println(tableName + "文件夹所用时间:" + (stopTime - startTime) + "毫秒");

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
        String s;
        List<String> records = new ArrayList<String>();
        s = readToString(file.toString());
        String[] split = s.split("\n");
        String tableName = "tdm" + file.getName().replace("onlinemonitordata_", "").replace("_sirui", "");
        for (int i1 = 1; i1 <= split.length; i1++) {
            try {
            String[] split2 = split[i1 - 1].split(" ");
            String date = split2[0] + " " + split2[1];
            //解析数据
            ArrayList arrayList1 = Transfrom.Transfrom_hot(split2[2]);
            //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            Long time = 0L;
            try {
                time = sdf.parse(date).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }


            Map<String, String> tags = new HashMap<String, String>();
            Map<String, Object> fields = new HashMap<String, Object>();

            fields.put("channel_1", arrayList1.get(0));
            fields.put("channel_2", arrayList1.get(1));
            fields.put("channel_3", arrayList1.get(2));

            // influxDBConnection.insert("rear_rotor_vibration_1",tags,fields,time,TimeUnit.MILLISECONDS);

            Point point = influxDBConnection.pointBuilder(tableName, time, tags, fields);
            BatchPoints batchPoints1 = BatchPoints.database("xiangtan")
                    .retentionPolicy("").consistency(InfluxDB.ConsistencyLevel.ALL).build();
            batchPoints1.point(point);

            records.add(batchPoints1.lineProtocol());

            }catch (Exception e){
                System.out.println(file.getPath());
                System.out.println(split[i1 - 1]);
            }
        }

        //long startTime = System.currentTimeMillis();
        try {

            influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
            file.delete();
        }catch (Exception e){
            System.out.println("绝缘过热入库失败");
            e.printStackTrace();
        }
        //long stopTime = System.currentTimeMillis();
        // System.out.println(tableName + "文件夹所用时间:" + (stopTime - startTime) + "毫秒");
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
        String s;
        List<String> records = new ArrayList<String>();
        s = readToString(file.toString());
        String[] split = s.split("\n");
        String tableName = "tdm" + file.getName().replace("onlinemonitordata_", "").replace("_sirui", "");

        for (int i1 = 1; i1 <= split.length; i1++) {
            try {
            String[] split2 = split[i1 - 1].split(" ");
            String date = split2[0] + " " + split2[1];
            Long time = 0L;
            try {
                time = sdf.parse(date).getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }


            Map<String, String> tags = new HashMap<String, String>();
            Map<String, Object> fields = new HashMap<String, Object>();
            // System.out.println("轴电流:"+split2[2].substring(40960,split2[2].length()));
            fields.put("channel", split2[2]);


            // influxDBConnection.insert("rear_rotor_vibration_1",tags,fields,time,TimeUnit.MILLISECONDS);


            Point point = influxDBConnection.pointBuilder(tableName, time, tags, fields);
            BatchPoints batchPoints1 = BatchPoints.database("xiangtan")
                    .retentionPolicy("").consistency(InfluxDB.ConsistencyLevel.ALL).build();
            batchPoints1.point(point);

            records.add(batchPoints1.lineProtocol());

            }catch (Exception e){
                System.out.println(file.getPath());
                System.out.println(split[i1 - 1]);
            }
        }

        //long startTime = System.currentTimeMillis();
        try {
            influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
            file.delete();
        }catch (Exception e){
           // System.out.println(file.getName()+"入库报错:"+e.getMessage());
            System.out.println("轴电流入库失败");
            e.printStackTrace();
        }
        //long stopTime = System.currentTimeMillis();
        //System.out.println(tableName + "文件夹所用时间:" + (stopTime - startTime) + "毫秒");
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
    private static void partial_discharge(Connection c, File file)  {
        try {
            String s;
            s = readToString(file.toString());
            String[] value = s.split("\n");
            Statement stmt = c.createStatement();
            String tableName="partial_discharge";
            String machine;
            if (file.getName().contains("1")){
                machine="1";
            }else if (file.getName().contains("2")){
                machine="2";
            }else {
                machine="3";
            }
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
        }catch (Exception e){
            System.out.println("局部放电入库失败");
            e.printStackTrace();
        }
    }
    private static void ReadInfluxDB(InfluxDBConnect influxDBConnection) {
        long startTime = System.currentTimeMillis();
        // QueryResult query = influxDBConnection.query("select SAMPLE(sensor1,5) from onlinemonitordata_1_tsziot_front_rotor_vibration where time>='2019-10-10T16:00:00Z' and time<='2019-10-11T15:59:59Z'");
         //QueryResult query = influxDBConnection.query("select count(sensor1) from tdm_shaft_electric_current_1 tz('Asia/Shanghai')");
      //QueryResult query = influxDBConnection.query("select last(sensor1) from onlinemonitordata_1_tsziot_front_rotor_vibration where time>='2019-10-11T15:50:00Z' and time<='2019-10-11T15:59:59Z' tz('Asia/Shanghai')");
      //
      QueryResult query = influxDBConnection.query("select sensor1 from onlinemonitordata_1_tsziot_front_rotor_vibration  where time>='2019-10-10T15:50:00Z' and time<='2019-10-11T15:59:59Z' order by time desc limit 2 tz('Asia/Shanghai')");
        //QueryResult query = influxDBConnection.query(" SELECT count(\"sensor1\") FROM \"onlinemonitordata_2_tsziot_bearing_current\" where time>='2019-10-10T16:00:00Z' and time<='2019-10-11T15:59:59Z'GROUP BY time(1h) tz('Asia/Shanghai')");

      // QueryResult query = influxDBConnection.query(" SELECT sensor4 FROM onlinemonitordata_1_tsziot_front_rotor_vibration where time>='2019-10-10T15:58:00Z' and time<='2019-10-10T15:59:59Z' tz('Asia/Shanghai')");
        QueryResult.Result result = query.getResults().get(0);
        long stopTime = System.currentTimeMillis();

        if (result.getSeries() != null) {
            List<List<Object>> valueList = result.getSeries().stream().map(QueryResult.Series::getValues)
                    .collect(Collectors.toList()).get(0);

            if (valueList != null && valueList.size() > 0) {

                for (List<Object> value : valueList) {
                    Map<String, String> map = new HashMap<String, String>();
                    // 数据库中字段1取值
                    String field1 = value.get(0) == null ? null : value.get(0).toString();
                   String field2 = value.get(1) == null ? null : value.get(1).toString();
                  // String field3 = value.get(2) == null ? null : value.get(2).toString();
                  // String field4 = value.get(3) == null ? null : value.get(3).toString();
                  // String field5 = value.get(4) == null ? null : value.get(4).toString();
                  // String field6 = value.get(5) == null ? null : value.get(5).toString();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                  System.out.println("time:" + field1);
               System.out.println("sersor1:" + field2);
              //   System.out.println("sersor2:" + field3);
              //  System.out.println("sersor3:" + field4);
              //  System.out.println("sersor4:" + field5);
              //  System.out.println("sersor5:" + field6);
                }
                System.out.println("读取所用时间:" + (stopTime - startTime) + "毫秒");
                System.out.println("返回数据"+valueList.size());
            }
        }
        influxDBConnection.close();

    }
}
