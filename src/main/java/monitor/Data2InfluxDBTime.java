package monitor;

import monitorhandler.DataConvertHandler;
import monitorhandler.Transfrom;
import monitorhandler.TransfromTime;
import monitorutil.*;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.QueryResult;

import java.io.*;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.DateTimeException;
import java.util.*;
import java.util.stream.Collectors;

import static monitorhandler.TransfromTime.ArrayCalculation;
import static monitorhandler.TransfromTime.doubleToString;

public class Data2InfluxDBTime {
    static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    static SimpleDateFormat sdft = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static PropertiesUtil propertiesUtil;
    //static final int timeInterval=86400000;
    static final long timeInterval=5524253355235420L;
    static {
        try {
            propertiesUtil = new PropertiesUtil();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //存储上一个文件每个机器每个通道的时间
    static java.sql.Connection c = null;
    static ArrayList<String> remove = new ArrayList<String>();

    //记录30S表最新入库的一条数据
    static HashMap<String, Long> timeSecond = new HashMap();
    static HashMap<String, Long> lastTime = new HashMap();
    static HashMap<String, Long> fileTime = new HashMap();


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
            PgsqlDataSource pgsqlDataSource = new PgsqlDataSource();
           c = pgsqlDataSource.getConnection();

            System.out.println("Opened database successfully");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }*/

    public static void main(String[] args) throws InterruptedException, SQLException, ParseException, IOException {
        //String arg = args[0];
        //String arg = "F:\\data\\test\\新建文件夹";
       // System.out.println(arg);
        String influx_db = propertiesUtil.readValue("influx_db");
       // String influx_url = propertiesUtil.readValue("influx_url");
        String influx_url = "http://192.168.120.102:8086";
        InfluxDBConnect influxDBConnection = new InfluxDBConnect("admin", "admin", influx_url, influx_db);
        long timeMillis = System.currentTimeMillis();
        ReadInfluxDB(influxDBConnection);
        long timeMillis2 = System.currentTimeMillis();
        System.out.println((timeMillis2-timeMillis)/1000);
   /*     influxDBConnection.deleteMeasurementData("delete from tdm_back_bearing_1_800ms");
        influxDBConnection.deleteMeasurementData("delete from tdm_back_bearing_2_800ms");
        influxDBConnection.deleteMeasurementData("delete from tdm_front_bearing_2_800ms");
        influxDBConnection.deleteMeasurementData("delete from tdm_front_bearing_1_800ms");
        influxDBConnection.deleteMeasurementData("delete from tdm_Insulating_material_overheating_2_800ms");
        influxDBConnection.deleteMeasurementData("delete from tdm_Insulating_material_overheating_1_800ms");
        influxDBConnection.deleteMeasurementData("delete from tdm_shaft_electric_current_1_800ms");
        influxDBConnection.deleteMeasurementData("delete from tdm_shaft_electric_current_2_800ms");*/
        //File file_History = new File(arg);
        //File[] files = file_History.listFiles();
     /*   for (int i = 1; i < files.length; i++) {
            File file = new File(files[i].toString());
            Thread.sleep(5000);
            //String file_path = propertiesUtil.readValue("file");

            List<String> collect = Arrays.stream(file.listFiles()).map(File::getName).collect(Collectors.toList());
            collect.retainAll(remove);
            if (collect.size() > 0) {
                long startTime = System.currentTimeMillis();
                file2TD(influxDBConnection, file);
                long stopTime = System.currentTimeMillis();
                Date date = new Date();
                String date_format = sdf.format(date);
                System.out.println(date_format + "文件夹所用时间:" + (stopTime - startTime) + "毫秒");
            }
        }*/
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
            if (machine.equals("onlinemonitordata_1_sirui_rear_rotor_vibration")&fileTime.getOrDefault(files[i].getName(),0L)<files[i].lastModified()) {
                //实时入库influxDB,30秒入库pgsql
                rotor_vibration(influxDBConnection, files[i], 34,"tdm_back_bearing_1_800ms", timeSecond, "tdm_back_bearing_1_30s");
                fileTime.put(files[i].getName(),files[i].lastModified());
            }
            if (machine.equals("onlinemonitordata_2_sirui_rear_rotor_vibration")&fileTime.getOrDefault(files[i].getName(),0L)<files[i].lastModified()) {
                //实时入库influxDB,30秒入库pgsql
                rotor_vibration(influxDBConnection, files[i], 34,"tdm_back_bearing_2_800ms", timeSecond, "tdm_back_bearing_2_30s");
                fileTime.put(files[i].getName(),files[i].lastModified());
            }
            if (machine.equals("onlinemonitordata_2_sirui_front_rotor_vibration")&fileTime.getOrDefault(files[i].getName(),0L)<files[i].lastModified()) {
                //实时入库influxDB,30秒入库pgsql
                rotor_vibration(influxDBConnection, files[i], 33,"tdm_front_bearing_2_800ms", timeSecond, "tdm_front_bearing_2_30s");
                fileTime.put(files[i].getName(),files[i].lastModified());
            }
            if (machine.equals("onlinemonitordata_1_sirui_front_rotor_vibration")&fileTime.getOrDefault(files[i].getName(),0L)<files[i].lastModified()) {
                //实时入库influxDB,30秒入库pgsql
                rotor_vibration(influxDBConnection, files[i], 33,"tdm_front_bearing_1_800ms", timeSecond, "tdm_front_bearing_1_30s");
                fileTime.put(files[i].getName(),files[i].lastModified());
            }
            //绝缘过热
            if (machine.equals("onlinemonitordata_2_sirui_insulation_overheating")&fileTime.getOrDefault(files[i].getName(),0L)<files[i].lastModified()) {
                //实时入库influxDB,30秒入库pgsql
                insulation_overheating(influxDBConnection, files[i], "tdm_Insulating_material_overheating_2_800ms", timeSecond, "tdm_Insulating_material_overheating_2_30s");
                fileTime.put(files[i].getName(),files[i].lastModified());
            }
            if (machine.equals("onlinemonitordata_1_sirui_insulation_overheating")&fileTime.getOrDefault(files[i].getName(),0L)<files[i].lastModified()) {
                //实时入库influxDB,30秒入库pgsql
                insulation_overheating(influxDBConnection, files[i], "tdm_Insulating_material_overheating_1_800ms", timeSecond, "tdm_Insulating_material_overheating_1_30s");
                fileTime.put(files[i].getName(),files[i].lastModified());
            }
            //轴电流
            if (machine.equals("onlinemonitordata_2_sirui_bearing_current")&fileTime.getOrDefault(files[i].getName(),0L)<files[i].lastModified()) {
                //实时入库influxDB,30秒入库pgsql
                bearing_current(influxDBConnection, files[i], "tdm_shaft_electric_current_2_800ms", timeSecond, "tdm_shaft_electric_current_2_30s");
                fileTime.put(files[i].getName(),files[i].lastModified());
            }
            if (machine.equals("onlinemonitordata_1_sirui_bearing_current")&fileTime.getOrDefault(files[i].getName(),0L)<files[i].lastModified()) {
                //实时入库influxDB,30秒入库pgsql
                bearing_current(influxDBConnection, files[i], "tdm_shaft_electric_current_1_800ms", timeSecond, "tdm_shaft_electric_current_1_30s");
                fileTime.put(files[i].getName(),files[i].lastModified());
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
     * 〈振动数据解析入库,读一个文件,对每条数据解析取平均值,最大值最小值入库influxDB,同时每30S入库pgsql一条数据〉
     *
     * @Param: [influxDBConnection, file, tableName, subTypeId]
     * @Return: [influxDBConnection, file, tableName, subTypeId]
     * @Author: 何鹏
     * @Date: 2019/10/15 13:54
     */
    public static void rotor_vibration(InfluxDBConnect influxDBConnection, File file, Integer subTypeId, String tableNameMillisecond, HashMap<String, Long> secondTimeMap2, String tableNameSecond) {

        List<String> records = new ArrayList<String>();
        //读取文件
        String line = readToString(file.toString());
        String[] split = line.split("\n");

        try {
        for (int i1 = 1; i1 <= split.length; i1++) {

                String[] split2 = split[i1 - 1].split(" ");
                String date = split2[0] + " " + split2[1];
                ArrayList<HashMap<String, Double>> arrayList1 = TransfromTime.Transfrom_shake(split2[2], subTypeId);
                // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                Long dataTime = sdf.parse(date).getTime();
                long nowTime = System.currentTimeMillis();
                long timeDifference = nowTime - dataTime;
                Long lastTime = secondTimeMap2.getOrDefault(file.getName(), 0L);
                //判断数据时间戳在20天以内
                if (dataTime <= nowTime & timeDifference < timeInterval) {


                    Map<String, String> tags = new HashMap<String, String>();
                    Map<String, Object> fields = new HashMap<String, Object>();
                    Double non_outgoing_x_axis_min=Double.parseDouble(doubleToString(arrayList1.get(0).get("最小值"),5));
                    Double non_outgoing_x_axis_max=Double.parseDouble(doubleToString(arrayList1.get(0).get("最大值"),5));
                    BigDecimal non_outgoing_x_axis_avg=new BigDecimal(doubleToString(arrayList1.get(0).get("平均值"),10));
                    Double non_outgoing_y_axis_min=Double.parseDouble(doubleToString(arrayList1.get(1).get("最小值"),5));
                    BigDecimal non_outgoing_y_axis_avg=new BigDecimal(doubleToString(arrayList1.get(1).get("平均值"),10));
                    Double non_outgoing_y_axis_max=Double.parseDouble(doubleToString(arrayList1.get(1).get("最大值"),5));
                    Double outgoing_x_axis_min=Double.parseDouble(doubleToString(arrayList1.get(2).get("最小值"),5));
                    Double outgoing_x_axis_max=Double.parseDouble(doubleToString(arrayList1.get(2).get("最大值"),5));
                    BigDecimal outgoing_x_axis_avg= new BigDecimal(doubleToString(arrayList1.get(2).get("平均值"),10));
                    Double outgoing_y_axis_min=Double.parseDouble(doubleToString(arrayList1.get(3).get("最小值"),5));
                    Double outgoing_y_axis_max=Double.parseDouble(doubleToString(arrayList1.get(3).get("最大值"),5));
                    BigDecimal outgoing_y_axis_avg= new BigDecimal(doubleToString(arrayList1.get(3).get("平均值"),10));
;
                    fields.put("non_outgoing_x_axis_min",non_outgoing_x_axis_min);
                    fields.put("non_outgoing_x_axis_max",non_outgoing_x_axis_max);
                    fields.put("non_outgoing_x_axis_avg",non_outgoing_x_axis_avg );
                    fields.put("non_outgoing_y_axis_min", non_outgoing_y_axis_min);
                    fields.put("non_outgoing_y_axis_avg", non_outgoing_y_axis_avg);
                    fields.put("non_outgoing_y_axis_max", non_outgoing_y_axis_max);
                    fields.put("outgoing_x_axis_min", outgoing_x_axis_min);
                    fields.put("outgoing_x_axis_max",outgoing_x_axis_max);
                    fields.put("outgoing_x_axis_avg", outgoing_x_axis_avg);
                    fields.put("outgoing_y_axis_min",outgoing_y_axis_min);
                    fields.put("outgoing_y_axis_max", outgoing_y_axis_max);
                    fields.put("outgoing_y_axis_avg", outgoing_y_axis_avg);
                    //震动解析数据剩余(解析未知)
                    // fields.put("sensor5", arrayList1.get(4));
                    // influxDBConnection.insert("rear_rotor_vibration_1",tags,fields,time,TimeUnit.MILLISECONDS);

                    Point point = influxDBConnection.pointBuilder(tableNameMillisecond, dataTime, tags, fields);
                    BatchPoints batchPoints1 = BatchPoints.database("xiangtan")
                            .retentionPolicy("").consistency(InfluxDB.ConsistencyLevel.ALL).build();
                    batchPoints1.point(point);

                    records.add(batchPoints1.lineProtocol());

                    //30秒统计入库pgsql,同时去掉了重复数据的可能
                    if ((dataTime - lastTime) >= 30000) {
                        Statement stmt = c.createStatement();
                        String sql = String.format("insert into %s (createtime,non_outgoing_x_axis_min,non_outgoing_x_axis_max,non_outgoing_x_axis_avg,non_outgoing_y_axis_min,non_outgoing_y_axis_max,non_outgoing_y_axis_avg,outgoing_x_axis_min,outgoing_x_axis_max,outgoing_x_axis_avg,outgoing_y_axis_min,outgoing_y_axis_max,outgoing_y_axis_avg) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tableNameSecond,"'"+sdf.format(sdf.parse(date))+"'",non_outgoing_x_axis_min,non_outgoing_x_axis_max,non_outgoing_x_axis_avg,non_outgoing_y_axis_min,non_outgoing_y_axis_max,non_outgoing_y_axis_avg,outgoing_x_axis_min,outgoing_x_axis_max,outgoing_x_axis_avg,outgoing_y_axis_min,outgoing_y_axis_max,outgoing_y_axis_avg );

                        stmt.execute(sql);
                        secondTimeMap2.put(file.getName(),dataTime);
                    }

                } else {
                    System.out.println("本次时间戳为:" + date);
                }


        }
        //数据入库
        influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
        //删除文件
       // file.delete();
        } catch (Exception e) {
            e.printStackTrace();
            //System.out.println(split[i1 - 1].toString());
        }

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
    private static void insulation_overheating(InfluxDBConnect influxDBConnection, File file, String tableNameMillisecond, HashMap<String, Long> secondTimeMap2, String tableNameSecond) {

        List<String> records = new ArrayList<String>();
        String line = readToString(file.toString());
        String[] split = line.split("\n");

        try {
        for (int i1 = 1; i1 <= split.length; i1++) {

                String[] split2 = split[i1 - 1].split(" ");
                String date = split2[0] + " " + split2[1];
                //解析数据
                ArrayList<HashMap<String, Double>> arrayList1 = TransfromTime.Transfrom_hot(split2[2]);
                //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                Long dataTime = 0L;
                try {
                    dataTime = sdf.parse(date).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                long nowTime = System.currentTimeMillis();
                long timeDifference = nowTime - dataTime;
                Long lastTime = secondTimeMap2.getOrDefault(file.getName(), 0L);
                //判断数据时间戳在20天以内
                if (dataTime <= nowTime & timeDifference < timeInterval) {

                    Map<String, String> tags = new HashMap<String, String>();
                    Map<String, Object> fields = new HashMap<String, Object>();
                    Double channel_1_min=Double.parseDouble(doubleToString(arrayList1.get(0).get("最小值"),10));
                    Double channel_1_max=Double.parseDouble(doubleToString(arrayList1.get(0).get("最大值"),10));
                    Double channel_1_avg=Double.parseDouble(doubleToString(arrayList1.get(0).get("平均值"),15));
                    Double channel_2_min=Double.parseDouble(doubleToString(arrayList1.get(1).get("最小值"),10));
                    Double channel_2_avg=Double.parseDouble(doubleToString(arrayList1.get(1).get("平均值"),15));
                    Double channel_2_max=Double.parseDouble(doubleToString(arrayList1.get(1).get("最大值"),10));
                    Double channel_3_min=Double.parseDouble(doubleToString(arrayList1.get(2).get("最小值"),10));
                    Double channel_3_max=Double.parseDouble(doubleToString(arrayList1.get(2).get("最大值"),10));
                    Double channel_3_avg=Double.parseDouble(doubleToString(arrayList1.get(2).get("平均值"),15));
                    fields.put("channel_1_min", channel_1_min);
                    fields.put("channel_1_max", channel_1_max);
                    fields.put("channel_1_avg", channel_1_avg);
                    fields.put("channel_2_min", channel_2_min);
                    fields.put("channel_2_avg", channel_2_avg);
                    fields.put("channel_2_max", channel_2_max);
                    fields.put("channel_3_min", channel_3_min);
                    fields.put("channel_3_max", channel_3_max);
                    fields.put("channel_3_avg",  channel_3_avg);

                    // influxDBConnection.insert("rear_rotor_vibration_1",tags,fields,time,TimeUnit.MILLISECONDS);

                    Point point = influxDBConnection.pointBuilder(tableNameMillisecond, dataTime, tags, fields);
                    BatchPoints batchPoints1 = BatchPoints.database("xiangtan")
                            .retentionPolicy("").consistency(InfluxDB.ConsistencyLevel.ALL).build();
                    batchPoints1.point(point);

                    records.add(batchPoints1.lineProtocol());

                    if ((dataTime - lastTime) >= 30000) {
                        Statement stmt = c.createStatement();
                        String sql = String.format("insert into %s (createtime,channel_1_min,channel_1_max,channel_1_avg,channel_2_min,channel_2_max,channel_2_avg,channel_3_min,channel_3_max,channel_3_avg) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tableNameSecond,"'"+sdf.format(sdf.parse(date))+"'",channel_1_min,channel_1_max,channel_1_avg,channel_2_min,channel_2_max,channel_2_avg,channel_3_min,channel_3_max,channel_3_avg);
                        stmt.execute(sql);
                        secondTimeMap2.put(file.getName(),dataTime);
                    }
                } else {
                    System.out.println("本次时间戳为:" + date);
                }

        }

        //long startTime = System.currentTimeMillis();
        influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
        //file.delete();
    } catch (Exception e) {
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
    private static void bearing_current(InfluxDBConnect influxDBConnection, File file, String tableNameMillisecond, HashMap<String, Long> secondTimeMap2, String tableNameSecond) {

        List<String> records = new ArrayList<String>();
        String line = readToString(file.toString());
        String[] split = (line != null) ? line.split("\n") : new String[0];
        try {
        for (int i1 = 1; i1 <= split.length; i1++) {

                String[] split2 = split[i1 - 1].split(" ");
                String date = split2[0] + " " + split2[1];
                Long dataTime = 0L;
                try {
                    dataTime = sdf.parse(date).getTime();
                } catch (ParseException e) {
                    e.printStackTrace();
                }

                long nowTime = System.currentTimeMillis();
                long timeDifference = nowTime - dataTime;
                Long lastTime = secondTimeMap2.getOrDefault(file.getName(), 0L);
                //判断数据时间戳在20天以内
                if (dataTime <= nowTime & timeDifference < timeInterval) {

                    Map<String, String> tags = new HashMap<String, String>();
                    Map<String, Object> fields = new HashMap<String, Object>();
                    double[] doubleArrayByStringVersionTwo = DataConvertHandler.getDoubleArrayByStringVersionTwo(split2[2]);
                    HashMap<String, Double> arrayList1 = ArrayCalculation(doubleArrayByStringVersionTwo);
                    Double channel_min=Double.parseDouble(doubleToString(arrayList1.get("最小值"),10));
                    Double channel_max=Double.parseDouble(doubleToString(arrayList1.get("最大值"),10));
                    Double channel_avg=Double.parseDouble(doubleToString(arrayList1.get("平均值"),15));
                    fields.put("channel_min", channel_min);
                    fields.put("channel_max", channel_max);
                    fields.put("channel_avg", channel_avg);
                    Point point = influxDBConnection.pointBuilder(tableNameMillisecond, dataTime, tags, fields);
                    BatchPoints batchPoints1 = BatchPoints.database("xiangtan")
                            .retentionPolicy("").consistency(InfluxDB.ConsistencyLevel.ALL).build();
                    batchPoints1.point(point);

                    records.add(batchPoints1.lineProtocol());
                    if ((dataTime - lastTime) >= 30000) {
                        Statement stmt = c.createStatement();
                        String sql = String.format("insert into %s (createtime,channel_min,channel_max,channel_avg) values(%s,%s,%s,%s)", tableNameSecond,"'"+sdf.format(sdf.parse(date))+"'",channel_min,channel_max,channel_avg);
                        stmt.execute(sql);
                        secondTimeMap2.put(file.getName(),dataTime);
                    }
                } else {
                    System.out.println("本次时间戳为:" + date);
                }

        }
        influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
       // file.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }

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
    private static void partial_discharge(java.sql.Connection c, File file, String tableName, String machine) throws SQLException, ParseException {
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

    private static void ReadInfluxDB(InfluxDBConnect influxDBConnection) throws ParseException {

        //QueryResult query = influxDBConnection.query("select count(sersor1) from onlinemonitordata_1_tsziot_front_rotor_vibration");
        QueryResult query = influxDBConnection.query("select * from tdm1_bearing_current ORDER BY time desc limit 1 tz('Asia/Shanghai')");
        QueryResult.Result result = query.getResults().get(0);

        if (result.getSeries() != null) {
            List<List<Object>> valueList = result.getSeries().stream().map(QueryResult.Series::getValues)
                    .collect(Collectors.toList()).get(0);
            if (valueList != null && valueList.size() > 0) {
                for (List<Object> value : valueList) {
                    Map<String, String> map = new HashMap<String, String>();
                    // 数据库中字段1取值
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                    for (int i = 0; i < value.size(); i++) {
                        if (i>0){
                            Object field1 = value.get(i) == null ? null : value.get(i);
                            //String bigDecimal =doubleToString(Double.parseDouble(field1.toString()),10);
                            System.out.println("第"+(i+1)+"个字段:"+field1);
                        }else {
                            String field1 = value.get(i) == null ? null : value.get(i).toString();
                            System.out.println("第"+(i+1)+"个字段:"+sdft.format(sdf.parse(field1)));
                        }

                    }

                }
            }
        }

        influxDBConnection.close();
    }
}
