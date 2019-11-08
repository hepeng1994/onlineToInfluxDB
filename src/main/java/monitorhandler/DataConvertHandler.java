package monitorhandler;

import monitorutil.DataUtil;

/**
 * Created by liuhaozhen on 2018/1/23.
 * 数据转换类
 */
public class DataConvertHandler {

    private static final Integer FRONT_ROTOR_SUBTYPE = 33;
    private static final Integer REAR_ROTOR_SUBTYPE = 34;

    /**
     * 将Float[]转成Float[]
     * 计算工程量
     */
    public static Float[] getEngineeringData(int[] adValue, Integer subTypeId){
        Float[] engineering = new Float[adValue.length];
        for(int i = 0; i < adValue.length; i++){
            Float part1 = DataUtil.CHV1 - DataUtil.CHV0;
            Float part2 = DataUtil.CHAD1 - DataUtil.CHAD0;
            Float part3 = adValue[i] - DataUtil.CHAD0;
            Float part4 = DataUtil.CHV0 - DataUtil.ZEROV;
            Float part5 = part1 / part2;
            Float part6 = part5 * part3;
            Float part7 = part6 + part4;

            if(subTypeId == FRONT_ROTOR_SUBTYPE)
                engineering[i] = part7 / DataUtil.FRONT_SENSITIVITY;
            else if(subTypeId == REAR_ROTOR_SUBTYPE)
                engineering[i] = part7 / DataUtil.REAR_SENSITIVITY;
        }
        return engineering;
    }


    /**
     * 将Float[]转成double[]
     * 将Float工程量转成double工程量
     */
    public static double[] getDoubleArray(Float[] value){
        double[] double_array = new double[value.length];
        for(int i = 0; i < value.length; i++){
            double d = Double.parseDouble(String.valueOf(value[i]));
            double_array[i] = d;
        }

        return double_array;
    }


    /**
     * 将string转成double[]
     * 直接将string转成double工程量
     * 适用于轴系振动
     */
    public static double[] getDoubleArrayByString(String string, Integer subTypeId){
        return getDoubleArray(getAverageData(getEngineeringData(getFloatValueByHex(string), subTypeId)));
}

    /**
     * 将byte[]转成double[]
     * 直接将byte[]转成double工程量
     * 适用于绝缘过热和轴电流
     */
    public static double[] getDoubleArrayByStringVersionTwo(String string){
        return getDoubleArray(getAverageData(getFloatValueByHexVersionTwo(string)));
    }


    /**
     * 将Float[]转成Float[]
     * 每一个数减去平均值
     */
    public static Float[] getAverageData(Float[] dataList){
        Float sum = 0f;
        for(int i = 0; i < dataList.length; i++){
            sum += dataList[i];
        }

        Float average = sum / dataList.length;

        for(int i = 0; i < dataList.length; i++){
            dataList[i] = dataList[i] - average;
        }

        return dataList;
    }


    /**
     * Float[]保留2位小数
     */
    public static Float[] getFloatWithTwoDigits(Float[] array){
        for(int i = 0 ; i < array.length; i++){
            array[i] = Math.round(array[i] * 100f) / 100f;
        }
        return array;
    }

    /**
     * 将double[]转成Double[]
     */
    public static Double[] getDoubleByDouble(double[] array){
        Double[] result = new Double[array.length];

        for(int i = 0; i < array.length; i++){
            result[i] = array[i];
        }
        return result;
    }

    /**
     * 16进制string转换成Float型数组
     * Ad值或工程量
     */
    public static int[] getFloatValueByHex(String hex){
        Integer length = 4;
        int count = hex.length() / length;
        int[] array = new int[count];

        for(int i = 0; i < count; i++){
            String string = hex.substring(i * length, (i + 1) * length);
            String substring = "";
            substring = substring + string.substring(2, 4);
            substring = substring + string.substring(0, 2);

            short value = (short)(Integer.valueOf(substring, 16) & 0xffff);
            int data = (int) value;
           // System.out.println(data);
            //data = (int)Long.parseLong(substring, 16);
            array[i] = data;
        }
        return array;
    }


    /**
     * 16进制string转换成Float型数组
     * Ad值或工程量
     */
    public static Float[] getFloatValueByHexVersionTwo(String hex){
        //每length长表示一个数据
        Integer length = 8;
        int count = hex.length() / length;
        Float[] array = new Float[count];

        for(int i = 0; i < count; i++){
            String string = hex.substring(i * length, (i + 1) * length);
            String substring = "";
            substring = substring + string.substring(6, 8);
            substring = substring + string.substring(4, 6);
            substring = substring + string.substring(2, 4);
            substring = substring + string.substring(0, 2);

            Long longValue = Long.parseLong(substring, 16);
            Float data = Float.intBitsToFloat(longValue.intValue());
            array[i] = data;
        }
        return array;
    }


    public static void main(String[] args){
//        String string = "3E1E9E9F";
//
//        String myString = "BEE9C77A";
//        Long i = Long.parseLong(myString, 16);
//        Float f = Float.intBitsToFloat(i.intValue());
//
//        Float value = Float.intBitsToFloat(Integer.valueOf(string.trim(), 16));
//
//        System.out.println(f);

        short value = (short)(Integer.valueOf("B898", 16) & 0xffff);
        Float part1 = DataUtil.CHV1 - DataUtil.CHV0;
        Float part2 = DataUtil.CHAD1 - DataUtil.CHAD0;
        Float part3 = value - DataUtil.CHAD0;
        Float part4 = DataUtil.CHV0 - DataUtil.ZEROV;
        Float part5 = part1 / part2;
        Float part6 = part5 * part3;
        Float part7 = part6 + part4;
        float float_value = part7 / DataUtil.SENSITIVITY;
        System.out.println(float_value);

//        System.out.println(onlineMonitorService.getLatestOnlinemonitordataOneSiruiFrontRotorVibrationData().getSensor1());
    }
}
