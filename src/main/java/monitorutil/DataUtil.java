package monitorutil;

/**
 * Created by 赵奇隆 on 2017/3/21.
 * 这个类存放一些常数类型
 */

public class DataUtil {

    public static Integer RESPONSE_INFO_OK = 1;
    public static Integer RESPONSE_INFO_DB_ERROR = 2;
    public static Integer RESPONSE_INFO_PARAM_LOST = 3;
    public static Integer RESPONSE_INFO_PARAM_FORMAT_ERROR = 4;
    public static Integer RESPONSE_INFO_PARAM_OUT_OF_RANGE = 5;
    public static Integer RESPONSE_INFO_OTHER_ERROR = 6;
    public static Integer RESPONSE_INFO_LOGIN_ERROR = 7;


    public static Integer THRESHOLD_EXCEPTION = 1;
    public static Integer OUTLIER_EXCEPTION = 2;
    public static Integer PREDICTION_EXCEPTION = 3;
    public static Integer TENDENCY_EXCEPTION = 4;
    public static Integer INTRUSION_EXCEPTION = 5;
    public static Integer LEAKTION_EXCEPTION = 6;
    public static Integer TRACKING_EXCEPTION = 7;


    //计算工程量所需参数
    //针对轴系
    public static Float CHV0 = 0f;
    public static Float CHV1 = 10000f;
    public static Float CHAD0 = 0f;
    public static Float CHAD1 = 16383f;
    public static Float ZEROV = 0f;
    public static Float SENSITIVITY = 8f;
    public static Float FRONT_SENSITIVITY = 7.87f;
    public static Float REAR_SENSITIVITY = 3.94f;

    //在线监测数据插入间隔
    public static Integer SECONDS = 60;
}
