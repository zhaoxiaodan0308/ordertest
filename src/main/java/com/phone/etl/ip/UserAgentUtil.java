package com.phone.etl.ip;

import org.apache.log4j.Logger;
import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;

import java.io.IOException;

/**
 * @ClassName UserAgentUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description
 *
 * window.navigator.userAgent
 **/
public class UserAgentUtil {
    public static final Logger logger = Logger.getLogger(UserAgentUtil.class);

    // 初始化一个浏览器信息类的对象
    static  UserAgentInfo result= new UserAgentInfo();



    public static UserAgentInfo parserUserAgent(String userAgent){

        if (userAgent.isEmpty() || userAgent.trim().isEmpty()){
            return  result;
        }

        try {
            UASparser uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());
            cz.mallat.uasparser.UserAgentInfo info = uasParser.parse(userAgent);

            result.setBrowserName(info.getUaName());
            result.setBrowserVersion(info.getBrowserVersionInfo());
            result.setOsName(info.getOsFamily());
            result.setOsVersion(info.getOsName());

        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * 用于封装useragent解析后的信息
     */
    public static class UserAgentInfo{
        private String browserName; // 浏览器名称
        private String browserVersion; // 浏览器版本号
        private String osName; // 操作系统名称
        private String osVersion; // 操作系统版本号

        public UserAgentInfo() {
        }

        public UserAgentInfo(String browserName, String browserVersion, String osName, String osVersion) {
            this.browserName = browserName;
            this.browserVersion = browserVersion;
            this.osName = osName;
            this.osVersion = osVersion;
        }

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }
    }
}