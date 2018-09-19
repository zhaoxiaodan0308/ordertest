package com.phone.etl.ip;

import com.phone.etl.IpUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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


    public static Map<String, UserInfo> parserLog(String log) {

        if(StringUtils.isEmpty(log)){
            return  null;
        }
        String[] strings = log.split("\\^A");
        String ip = strings[0];
        String time = strings[1];

        String varlines = strings[3];
        String[] split = varlines.split("b_iev=");
        String userAgent = split[1];


        //country\province\city
        IpUtil.RegionInfo regionInfoByIp = IpUtil.getRegionInfoByIp(ip);

        //时间戳
        String times = time.replace("\\.", "");

        //浏览器名称\ 浏览器版本号\操作系统名称\操作系统版本号
        UserAgentUtil.UserAgentInfo userAgentInfo = UserAgentUtil.parserUserAgent(userAgent);

        Map<String, UserInfo> map = new HashMap();
        UserInfo userInfo=new UserInfo();
        userInfo.setUserAgentInfo(userAgentInfo);
        userInfo.setLocation(regionInfoByIp);
        userInfo.setTimeStamp(times);

        map.put(times, userInfo);
        return map;
    }

    public static class UserInfo implements Writable {
        private String timeStamp;
        private String country;
        private String province;
        private String city;
        private String browserName; // 浏览器名称
        private String browserVersion; // 浏览器版本号
        private String osName; // 操作系统名称
        private String osVersion; // 操作系统版本号

        public String getTimeStamp() {
            return timeStamp;
        }

        public String getCountry() {
            return country;
        }

        public String getProvince() {
            return province;
        }

        public String getCity() {
            return city;
        }

        public String getBrowserName() {
            return browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setTimeStamp(String timeStamp) {
            this.timeStamp = timeStamp;
        }

        public void setLocation(IpUtil.RegionInfo regionInfoByIp) {
            this.country = regionInfoByIp.getCountry();
            this.province = regionInfoByIp.getProvince();
            this.city = regionInfoByIp.getCity();
        }


        public void setUserAgentInfo(UserAgentUtil.UserAgentInfo userAgentInfo) {
            this.browserName = userAgentInfo.getBrowserName();
            this.browserVersion = userAgentInfo.getBrowserVersion();
            this.osName = userAgentInfo.getOsName();
            this.osVersion = userAgentInfo.getOsVersion();
        }


        public UserInfo(String timeStamp, String country, String province,
                        String city, String browserName, String browserVersion,
                        String osName, String osVersion) {
            this.timeStamp = timeStamp;
            this.country = country;
            this.province = province;
            this.city = city;
            this.browserName = browserName;
            this.browserVersion = browserVersion;
            this.osName = osName;
            this.osVersion = osVersion;
        }

        public UserInfo() {
        }

        @Override
        public String toString() {
            return timeStamp + "\t" +
                    country + "\t" +
                    province + "\t" +
                    city + "\t" +
                    browserName + "\t" +
                    browserVersion + "\t" +
                    osName + "\t" +
                    osVersion;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(timeStamp);
            dataOutput.writeUTF(country);
            dataOutput.writeUTF(province);
            dataOutput.writeUTF(city);
            dataOutput.writeUTF(browserName);
            dataOutput.writeUTF(browserVersion);
            dataOutput.writeUTF(osName);
            dataOutput.writeUTF(osVersion);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            timeStamp=dataInput.readUTF();
            country=dataInput.readUTF();
            province=dataInput.readUTF();
            city=dataInput.readUTF();
            browserName=dataInput.readUTF();
            browserVersion=dataInput.readUTF();
            osName=dataInput.readUTF();
            osVersion=dataInput.readUTF();
        }
    }
}

