package monitorutil;

import org.influxdb.InfluxDB;
import org.influxdb.dto.QueryResult;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
/**
 * 功能描述: <br>
 * 〈inflxuDB获取数据〉
 * @Author: 何鹏
 * @Date: 2019/11/6 13:31
 */
public class InfluxDBGetData {

    static String influx_url = "http://192.168.31.210:8086";
    static String influx_db;
    static PropertiesUtil propertiesUtil;

    static {
        try {
            propertiesUtil = new PropertiesUtil();
            influx_db = propertiesUtil.readValue("influx_db");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void InfluxDBGetActualTime(InfluxDBConnect influxDBConnect) {
        long startTime = System.currentTimeMillis();
        //QueryResult query = influxDBConnect.query("select SAMPLE(\"sensor2\",10) from \"onlinemonitordata_1_tsziot_front_rotor_vibration\" where time>='2019-10-11T15:00:00Z' and time<='2019-10-11T15:59:59Z'");
        QueryResult query = influxDBConnect.query("select SAMPLE(sensor3,100) from onlinemonitordata_1_tsziot_front_rotor_vibration where time>='2019-10-11T15:00:00Z' and time<='2019-10-11T15:59:59Z'");
       // QueryResult query = influxDBConnect.query("select sensor3 from onlinemonitordata_1_tsziot_front_rotor_vibration where time>='2019-10-11T15:00:00Z' and time<='2019-10-11T15:59:59Z' limit 500 tz('Asia/Shanghai')");
        // QueryResult query = influxDBConnect.query("select last(sensor2) from onlinemonitordata_1_tsziot_front_rotor_vibration where time>='2019-10-10T15:58:00Z' and time<='2019-10-11T15:59:59Z' tz('Asia/Shanghai')");
         //QueryResult query = influxDBConnect.query("select last(sensor3) from onlinemonitordata_1_tsziot_front_rotor_vibration where time > now() - 4w tz('Asia/Shanghai')");
        //
        //QueryResult query = influxDBConnection.query("select sensor1 from onlinemonitordata_1_tsziot_front_rotor_vibration  where time>='2019-10-11T15:50:00Z' and time<='2019-10-11T15:59:59Z' order by time desc limit 1 tz('Asia/Shanghai')");
        //QueryResult query = influxDBConnect.query(" SELECT count(\"sensor1\") FROM \"onlinemonitordata_2_tsziot_bearing_current\" where time>='2019-10-10T16:00:00Z' and time<='2019-10-11T15:59:59Z'GROUP BY time(1h) tz('Asia/Shanghai')");

        // QueryResult query = influxDBConnection.query(" SELECT * FROM onlinemonitordata_1_tsziot_front_rotor_vibration where time>='2019-10-10T15:58:00Z' and time<='2019-10-10T15:59:59Z' tz('Asia/Shanghai')");
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
    }

    public static void main(String[] args) {
        InfluxDBConnect influxDBConnect= new InfluxDBConnect("admin", "admin", influx_url, influx_db);

        InfluxDBGetActualTime(influxDBConnect);

    }
}
