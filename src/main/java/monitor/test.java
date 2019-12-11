package monitor;

import monitorutil.PgsqlDataSource;
import sun.java2d.pipe.SpanIterator;

import java.io.Console;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static monitor.Data2IfluxDB.sdf;

public class test {
    public static void main(String[] args) throws IOException, ParseException, SQLException {
      /*  String influx_db = propertiesUtil.readValue("influx_db");
        String influx_url = propertiesUtil.readValue("influx_url");
        InfluxDBConnection influxDBConnection = new InfluxDBConnection("admin", "admin", influx_url, influx_db, "");

        File file = new File("F:\\data\\新建文件夹\\onlinemonitordata_2_sirui_rear_rotor_vibration");
     rotor_vibration(influxDBConnection,file);

    }
    public static void rotor_vibration(InfluxDBConnection influxDBConnection, File file) {

        String s;
        List<String> records = new ArrayList<String>();
        //读取文件
        s = readToString(file.toString());
        String[] split = s.split("\n");

        for (int i1 = 1; i1 <= split.length; i1++) {
            *//*try {*//*
                String[] split2 = split[i1 - 1].split(" ");
                String date = split2[0] + " " + split2[1];
                ArrayList arrayList1 = Transfrom.Transfrom_shake(split2[2]);
                // SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                Long time = 0L;
                try {
                time = sdf.parse(date).getTime();
                } catch (Exception e) {
                    System.out.println(split[i1 - 1].toString());
                }

                Map<String, String> tags = new HashMap<String, String>();
                Map<String, Object> fields = new HashMap<String, Object>();

                fields.put("serson1", arrayList1.get(0));
                fields.put("serson2", arrayList1.get(1));
                fields.put("serson3", arrayList1.get(2));
                fields.put("serson4", arrayList1.get(3));
                //震动解析数据剩余(解析未知)
                fields.put("serson5", arrayList1.get(4));
                // influxDBConnection.insert("rear_rotor_vibration_1",tags,fields,time,TimeUnit.MILLISECONDS);

                Point point = influxDBConnection.pointBuilder(file.getName().replace("sirui","tsziot"), time, tags, fields);
                BatchPoints batchPoints1 = BatchPoints.database("xiangtan")
                        .retentionPolicy("").consistency(InfluxDB.ConsistencyLevel.ALL).build();
                batchPoints1.point(point);

                records.add(batchPoints1.lineProtocol());

       *//*     if (i1 % 100 == 0) {

                long startTime = System.currentTimeMillis();

                influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
                records.clear();
                long stopTime = System.currentTimeMillis();
                System.out.println("文件夹所用时间:" + (stopTime - startTime) + "毫秒");
            }*//*
        *//*    } catch (Exception e) {
                System.out.println(split[i1 - 1].toString());
            }*//*
        }

        // long startTime = System.currentTimeMillis();
        influxDBConnection.batchInsert("xiangtan", "", InfluxDB.ConsistencyLevel.ALL, records);
       // file.delete();
        // long stopTime = System.currentTimeMillis();
        // System.out.println(tableName + "文件夹所用时间:" + (stopTime - startTime) + "毫秒");

*/

        /*Date date = new Date();
        System.out.println(date);

        String format = sdf.format(date);
        System.out.println(format);*/
        //Power2Pgsql.IntradayAdjustment("C:\\Users\\ASUS\\Desktop\\数据\\送端功率\\日内调整\\20190815\\祁韶直流_20190815012833.JHTZ");
       /*  ArrayList<String> remove =new ArrayList<String>();

            remove.add("onlinemonitordata_1_sirui_bearing_current");
            remove.add("onlinemonitordata_1_sirui_front_rotor_vibration");
            remove.add("onlinemonitordata_1_sirui_insulation_overheating");
            remove.add("onlinemonitordata_1_sirui_rear_rotor_vibration");
            remove.add("onlinemonitordata_2_sirui_bearing_current");
            remove.add("onlinemonitordata_2_sirui_front_rotor_vibration");
            remove.add("onlinemonitordata_2_sirui_insulation_overheating");
            remove.add("onlinemonitordata_2_sirui_rear_rotor_vibration");
        for (int i = 0; i < remove.size(); i++) {
            String s = "tdm" +  remove.get(i).replace("onlinemonitordata_", "").replace("_sirui", "");
            String  millisecondTableName = "tdm" + remove.get(i).replace("onlinemonitordata_", "").replace("_sirui", "")+"_800ms";
            String secondTableName = "tdm" + remove.get(i).replace("onlinemonitordata_", "").replace("_sirui", "")+"_30s";

            System.out.println(s+","+millisecondTableName+","+secondTableName);
        }*/
       //String a="onlinemonitordata_1_sirui_rear_rotor_vibration";
       //String s = "tdm" + a.replace("onlinemonitordata_", "").replace("_sirui", "");
       //System.out.println(s);
        //tdm1_rear_rotor_vibration
        /*  Connection conn = null;
        PreparedStatement pstmt= null;
        PgsqlDataSource datasource = new PgsqlDataSource();
        System.out.println(datasource);
        try {
            conn = datasource.getConnection();
            String sql = "insert into tbl_user values(null,?,?)";
            pstmt =  conn.prepareStatement(sql);
            pstmt.setString(1, "hello");
            pstmt.setString(2, "java");
            int row=pstmt.executeUpdate();
            if(row>0) {
                System.out.println("添加成功");
            }else {
                System.out.println("添加失败");
            }
        }catch(Exception e) {
            throw new RuntimeException(e);
        }finally {
            datasource.backConnection(conn);
        }*/


    }


}
