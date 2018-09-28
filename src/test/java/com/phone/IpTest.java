package com.phone;

import com.phone.etl.ip.LogUtil;

import java.util.Map;

/**
 * @ClassName IpTest
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description //TODO $
 **/
public class IpTest {
    public static void main(String[] args) {
//        System.out.println(IPSeeker.getInstance().getCountry("112.111.11.12"));
//
//        System.out.println(IpUtil.getRegionInfoByIp("112.111.11.12"));
//
//        try {
//            System.out.println(IpUtil.parserIp1("http://ip.taobao.com/service/getIpInfo.php?ip=112.111.11.12","utf-8"));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println(UserAgentUtil.parserUserAgent("\"Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\""));
        Map<String, String> stringStringMap = LogUtil.parserLog("114.61.94.253^A1537237000.456^Ahh^A/BCImg.gif?en=e_pv&p_url=http%3A%2F%2Flocalhost%3A8080%2Fbf_track_jssdk%2Fdemo4.jsp&p_ref=http%3A%2F%2Flocalhost%3A8080%2Fbf_track_jssdk%2Fdemo.jsp&tt=%E6%B5%8B%E8%AF%95%E9%A1%B5%E9%9D%A24&ver=1&pl=java_server&sdk=js&u_ud=27F69684-BBE3-42FA-AA62-71F98E208444&u_mid=Aidon&u_sd=38F66FBB-C6D6-4C1C-8E05-72C31675C00A&c_time=1449917532456&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F46.0.2490.71%20Safari%2F537.36&b_rst=1280*768");
        for (Map.Entry map : stringStringMap.entrySet()) {
            System.out.println(map.getValue());
        }

    }
}