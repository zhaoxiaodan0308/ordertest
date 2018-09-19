package com.phone.etl.ip;

import com.phone.common.Constants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName LogUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class LogUtil {

    private static final Logger logger = Logger.getLogger(LogUtil.class);

    public static Map<String, String> parserLog(String log) {

        Map<String, String> map = new HashMap<>();

        if (StringUtils.isEmpty(log)) {
            return null;
        }

        String[] strings = log.split("\\^A");
        //日志由四部分组成
        if (strings.length == 4) {
            //ip
            map.put(Constants.LOG_IP, strings[0]);
            //存储ip解析的地区
            handleIP(map);
            //时间戳
            map.put(Constants.LOG_SERVER_TIME, strings[1].replace("\\.", ""));
            //输入参数解析
            String parms = strings[3];
            handleParms(parms, map);
            //userAget
            handleUserAgent(map);
        }

        return map;
    }


    //根据ip获取地区信息：国家、省、区 存储进map
    private static void handleIP(Map<String, String> map) {
        if (map.containsKey(Constants.LOG_IP)) {
            IpUtil.RegionInfo regionInfoByIp = IpUtil.getRegionInfoByIp(map.get(Constants.LOG_IP));
            map.put(Constants.LOG_COUNTRY, regionInfoByIp.getCountry());
            map.put(Constants.LOG_PROVINCE, regionInfoByIp.getProvince());
            map.put(Constants.LOG_CITY, regionInfoByIp.getCity());
        }
    }

    //解析输入参数的信息

    /**
     * 处理正行的日志
     * :221.13.21.192^A1535611950.612^A221.13.21.192^A/
     * shopping.jsp?c_time=1535611600666&oid=123461&u_mid=dc64823d-5cb7-4e3d-8a87-fa2b4e096ea0&pl=java_server&en=e_cs&sdk=jdk&ver=1
     */
    private static void handleParms(String parms, Map<String, String> map) {
        try {
            if (StringUtils.isNotEmpty(parms)) {
                if (parms.contains("?")) {

                    String[] split = parms.split("\\?");
                    String[] kvs = split[1].split("&");

                    for (String e : kvs) {
                        String[] kv = e.split("=");
                        String k = kv[0];
                        String v = URLDecoder.decode(kv[1], "utf-8");
                        if (StringUtils.isNotEmpty(k)) {
                            map.put(k, v);
                        }

                    }
                }
            }

        } catch (UnsupportedEncodingException e) {
            logger.error("解析参数数值错误");
        }
    }

    /**
     * 解析userAgent
     *
     * @param map
     */
    private static void handleUserAgent(Map<String, String> map) {
        if (map.containsKey(Constants.LOG_USERAGENT)) {
            UserAgentUtil.UserAgentInfo info = UserAgentUtil.parserUserAgent(map.get(Constants.LOG_USERAGENT));
            //将值存储到map中
            map.put(Constants.LOG_BROWSER_NAME, info.getBrowserName());
            map.put(Constants.LOG_BROWSER_VERSION, info.getBrowserVersion());
            map.put(Constants.LOG_OS_NAME, info.getOsName());
            map.put(Constants.LOG_OS_VERSION, info.getOsVersion());
        }
    }

}

