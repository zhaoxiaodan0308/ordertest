package com.phone;

import java.io.*;
import java.util.LinkedList;

/**
 * @ClassName IpUtil
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 获取每一行日志的Ip，判断所在ip段，然后返回对应的code
 **/
public class IpUtil {

    /**
     * ip地址转成long型数字
     * 将IP地址转化成整数的方法如下：
     * 1、通过String的split方法按.分隔得到4个长度的数组
     * 2、通过左移位操作（<<）给每一段的数字加权，第一段的权为2的24次方，第二段的权为2的16次方，第三段的权为2的8次方，最后一段的权为1
     * @param strIp
     * @return
     */
    public static long ipToLong(String strIp) {
        String[] ip = strIp.split("\\.");
        long res = (Long.valueOf(ip[0]) << 24) + (Long.valueOf(ip[1]) << 16)
                + (Long.valueOf(ip[2]) << 8) + Long.valueOf(ip[3]);
        return res;
    }


    /**
     * 根据ip地址判断属于哪个ip地址段，返回这个段的ip地址对象
     * 使用二分查找算法来实现
     * @param ip
     * @param
     * @return
     */
    public static IPBean getCodeByIp(String ip) {
        IPBean[] ipBeans = getIpBeans();
        if (ipBeans == null || ipBeans.length == 0)
            return null;
        long iplong = ipToLong(ip);
        if (iplong < ipBeans[0].getBegin() || iplong > ipBeans[ipBeans.length - 1].getEnd())
            return null;
        int left = 0;
        int right = ipBeans.length - 1;
        int mid = (left + right) / 2;
        while (left <= right) {
            if (iplong < ipBeans[mid].getBegin())
                right = mid - 1;
            if (iplong > ipBeans[mid].getBegin())
                left = mid + 1;
            if (iplong >= ipBeans[mid].getBegin() && iplong <= ipBeans[mid].getEnd())
                return ipBeans[mid];
            mid = (left + right) / 2;
        }
        return null;
    }

    /**
     * 读取ipcode.txt文件，按行读取，并转成IPBean对象数组
     * @return
     */
    public static IPBean[] getIpBeans() {
        InputStreamReader inputStreamReader = null;
        BufferedReader reader = null;
        try {
            File file = new File("F://programs/phone_analystic/src/testdata/ip_rules");
            inputStreamReader = new InputStreamReader(new FileInputStream(file), "UTF-8");
            reader = new BufferedReader(inputStreamReader);
            String line;
            LinkedList<IPBean> ipBeanList = new LinkedList<IPBean>();
            while ((line = reader.readLine()) != null) {
                String[] tmp = line.split(" ");
                ipBeanList.add(new IPBean(ipToLong(tmp[0]), ipToLong(tmp[1]), tmp[2]));
            }
            IPBean[] ipBeans = ipBeanList.toArray(new IPBean[] {});
            return ipBeans;
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            try {
                reader.close();
                inputStreamReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    /**
     * 封装ip规则文件中的信息
     */
    public static class IPBean {
        private long begin;
        private long end;
        private String code;

        public IPBean() {
        }

        public long getBegin() {
            return begin;
        }

        public void setBegin(long begin) {
            this.begin = begin;
        }

        public long getEnd() {
            return end;
        }

        public void setEnd(long end) {
            this.end = end;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public IPBean(long begin, long end, String code) {
            this.begin = begin;
            this.end = end;
            this.code = code;
        }

        @Override
        public String toString() {
            return "IPBean [begin=" + begin + ", end=" + end + ", code=" + code + "]";
        }

    }

    public static void main(String[] args) {
        IPBean ipBean = getCodeByIp("0.0.0.0");
        System.out.println(ipBean);
    }

}