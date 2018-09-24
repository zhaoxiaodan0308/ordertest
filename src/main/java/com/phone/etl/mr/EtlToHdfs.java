package com.phone.etl.mr;

import com.phone.common.Constants;
import com.phone.etl.ip.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

/**
 * @ClassName EtlToHdfs
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description 原数据：/log/09/18
 * 原数据：/log/09/19
 * 清洗后的存储目录: /ods/09/18
 * 清洗后的存储目录: /ods/09/19
 **/
public class EtlToHdfs extends Mapper<LongWritable, Text, LogWritable, NullWritable> {
    private static final Logger logger = Logger.getLogger(EtlToHdfs.class);
    private LogWritable k = new LogWritable();
    private static int inputRecords, filterRecords, outputRecords = 0;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String values = value.toString();
        inputRecords++;
        //判断输入是否为空
        if (StringUtils.isEmpty(values)) {
            filterRecords++;
            return;
        }

        Map<String, String> map = LogUtil.parserLog(values);

        //所有的事件输出到同一个文件中
        //获取事件名
        String eventName = map.get(Constants.LOG_EVENT_NAME);
        Constants.EventEnum eventEnum = Constants.EventEnum.valueOfAlias(eventName);

        switch (eventEnum) {
            case LANUCH:
            case EVENT:
            case PAGEVIEW:
            case CHARGEREFUND:
            case CHARGEREQUEST:
            case CHARGESUCCESS:
                handleMap(map, context);
                outputRecords++;
                break;
            default:
                break;
        }

    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.out.println("inpitRecord:" + inputRecords + " filterRecords:" + filterRecords
                + "outputRecords:" + outputRecords);
    }

    //将从日志中获取的map数据存放到bean中并输出
    private void handleMap(Map<String, String> map, Context context) {
        try {
            for (Map.Entry<String, String> en : map.entrySet()) {

                switch (en.getKey()) {
                    case "ver":
                        k.setVer(en.getValue());
                        break;
                    case "s_time":
                        this.k.setS_time(en.getValue());
                        break;
                    case "en":
                        this.k.setEn(en.getValue());
                        break;
                    case "u_ud":
                        this.k.setU_ud(en.getValue());
                        break;
                    case "u_mid":
                        this.k.setU_mid(en.getValue());
                        break;
                    case "u_sd":
                        this.k.setU_sd(en.getValue());
                        break;
                    case "c_time":
                        this.k.setC_time(en.getValue());
                        break;
                    case "l":
                        this.k.setL(en.getValue());
                        break;
                    case "b_iev":
                        this.k.setB_iev(en.getValue());
                        break;
                    case "b_rst":
                        this.k.setB_rst(en.getValue());
                        break;
                    case "p_url":
                        this.k.setP_url(en.getValue());
                        break;
                    case "p_ref":
                        this.k.setP_ref(en.getValue());
                        break;
                    case "tt":
                        this.k.setTt(en.getValue());
                        break;
                    case "pl":
                        this.k.setPl(en.getValue());
                        break;
                    case "ip":
                        this.k.setIp(en.getValue());
                        break;
                    case "oid":
                        this.k.setOid(en.getValue());
                        break;
                    case "on":
                        this.k.setOn(en.getValue());
                        break;
                    case "cua":
                        this.k.setCua(en.getValue());
                        break;
                    case "cut":
                        this.k.setCut(en.getValue());
                        break;
                    case "pt":
                        this.k.setPt(en.getValue());
                        break;
                    case "ca":
                        this.k.setCa(en.getValue());
                        break;
                    case "ac":
                        this.k.setAc(en.getValue());
                        break;
                    case "kv_":
                        this.k.setKv_(en.getValue());
                        break;
                    case "du":
                        this.k.setDu(en.getValue());
                        break;
                    case "browserName":
                        this.k.setBrowserName(en.getValue());
                        break;
                    case "browserVersion":
                        this.k.setBrowserVersion(en.getValue());
                        break;
                    case "osName":
                        this.k.setOsName(en.getValue());
                        break;
                    case "osVersion":
                        this.k.setOsVersion(en.getValue());
                        break;
                    case "country":
                        this.k.setCountry(en.getValue());
                        break;
                    case "province":
                        this.k.setProvince(en.getValue());
                        break;
                    case "city":
                        this.k.setCity(en.getValue());
                        break;
                }
            }

            context.write(k,NullWritable.get());
        } catch (Exception e) {
            logger.error("etl最终输出异常");
        }

    }

}
