package monitorutil;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import okhttp3.OkHttpClient;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

/**
  * 时序数据库 InfluxDB 连接
  * @author wangshumin
  *高并发    批量写  缓存50000  处理 数量单线程10最大()  抖动时间3毫秒 写入时间 最大500毫秒  最大100个线程运行
  */
public class InfluxDBConnect {
    private String username;//用户名
    private String password;//密码
    private String openurl;//连接地址
    private String database;//数据库
    private InfluxDB influxDB;
    private  final  int coreNum =  Runtime.getRuntime().availableProcessors();
    private  static  final Object obj = new Object();
    private    final BatchOptions batchOptions;
    private    final ThreadFactory threadFactory;
    //设置超时间
    static OkHttpClient.Builder client = new OkHttpClient.Builder()
            .readTimeout(500,TimeUnit.SECONDS);
    public InfluxDBConnect(String username, String password, String openurl, String database){
        this.username = username;
        this.password = password;
        this.openurl = openurl;
        this.database = database;
        batchOptions= BatchOptions.DEFAULTS;
        this.threadFactory = new ThreadPoolExecutor(coreNum, coreNum*2, 70, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>(4)).getThreadFactory();
        influxDbBuild();
    }
    /**连接时序数据库；获得InfluxDB**/

    public InfluxDB  influxDbBuild(){
        if(influxDB == null){
            synchronized (obj) {
                if(influxDB == null) {
                    //数量线程10 缓存 50000  抖动时间 3毫秒 最大写入时间 500毫秒    threadFactory  最大10个线程
                    batchOptions.actions(10).bufferLimit(50000).flushDuration(1000).jitterDuration(3).threadFactory(threadFactory);
                    influxDB = InfluxDBFactory.connect(openurl, username, password,client).enableBatch(batchOptions);
                    if(!influxDB.databaseExists(database)) {
                         influxDB.createDatabase(database);
                         this.createRetentionPolicy(null,null,null,null,false);
                    }
                }
            }
        }
        return influxDB;
    }

    /**
      * 设置数据保存策略
      * defalut 策略名 /database 数据库名/ 30d 数据保存时限30天/ 1  副本个数为1/ 结尾DEFAULT 表示 设为默认的策略
      */
            public void createRetentionPolicy(String RetentionPolicyName,String Ruration ,String Replication,String Database,Boolean flag){
        String command;
        if(!flag) {
          command = String.format("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION %s DEFAULT",
                                       "aRetentionPolicy", database, "30d", 1);  
       }else {
           command = String.format("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION %s DEFAULT",
                                       RetentionPolicyName, Database, Ruration, Replication);  
       }
        this.query(command);
    }

    /**
      * 查询
      * @param command 查询语句
      * @return
      */
    public QueryResult query(String command) {
        return influxDB.query(new Query(command,database));
    }


    /**
      * 批量插入
      * @param measurement 表
      * @param tags 标签map
      * @param fields 字段map
      */
            public void insertMany(String measurement, List<Map<Map<String, String>,Map<String, Object>>> tagsfieldsList){
        BatchPoints batchPoints = BatchPoints.database(database).consistency(ConsistencyLevel.ONE).precision(TimeUnit.MILLISECONDS).build();
        Object[] tagsfieldsMap = tagsfieldsList.toArray();
        for (int i = 0; i < tagsfieldsMap.length; i++) {
            Map.Entry<Map<String, String>,Map<String, Object>> tagsfields = (Map.Entry<Map<String, String>,Map<String, Object>>)tagsfieldsMap[i];
            Point point = Point.measurement(measurement).tag(tagsfields.getKey()).fields(tagsfields.getValue()).build();
            batchPoints.point(point);
        }
        influxDB.write(batchPoints);
    }
    /**
     * 批量写入数据
     *
     * @param database
     *            数据库
     * @param retentionPolicy
     *            保存策略
     * @param consistency
     *            一致性
     * @param records
     *            要保存的数据（调用BatchPoints.lineProtocol()可得到一条record）
     */
    public void batchInsert(final String database, final String retentionPolicy, final ConsistencyLevel consistency,
                            final List<String> records) {
        influxDB.write(database, retentionPolicy, consistency, records);
    }
    /**
     * 构建Point
     *
     * @param measurement
     * @param time
     * @param fields
     * @return
     */
    public Point pointBuilder(String measurement, long time, Map<String, String> tags, Map<String, Object> fields) {
        Point point = Point.measurement(measurement).time(time, TimeUnit.MILLISECONDS).tag(tags).fields(fields).build();
        return point;
    }
    /**
      * 单条插入
      * @param measurement 表
      * @param tags 标签
      * @param fields 字段
      */
            public void insert(String measurement, Map<String, String> tags, Map<String, Object> fields){
        Builder builder = Point.measurement(measurement);
        builder.tag(tags);
        builder.fields(fields);
        influxDB.write(database, "aRetentionPolicy", builder.build());
    }

    /**
      * 删除
      * @param command 删除语句
      * @return 返回错误信息
      */
            public String deleteMeasurementData(String command){
        QueryResult result = influxDB.query(new Query(command, database));
        return result.getError();
    }

    /**
      * 创建数据库
      * @param dbName
      */
            public void createDB(String dbName){
        influxDB.createDatabase(dbName);
    }

    /**
      * 删除数据库
      * @param dbName
      */
            public void deleteDB(String dbName){
        influxDB.deleteDatabase(dbName);
    }
    /**
     * 关闭数据库
     */
    public void close() {
        influxDB.close();
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getOpenurl() {
        return openurl;
    }

    public void setOpenurl(String openurl) {
        this.openurl = openurl;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
}
