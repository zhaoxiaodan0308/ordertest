package com.phone.analystic.mr.pv;

import com.phone.analystic.modle.StatsCommonDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.BrowserDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.base.KpiDimension;
import com.phone.analystic.modle.base.PlatformDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.common.Constants;
import com.phone.common.DateEnum;
import com.phone.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 用户模块下的新增用户
 */
public class PvMapper extends Mapper<LongWritable, Text, StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(PvMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension pvKpi = new KpiDimension(KpiType.PV.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            return;
        }

        //拆分
        String[] fields = line.split("\u0001");
        String en = fields[2];

        if (en.equals(Constants.EventEnum.PAGEVIEW.alias)) {
           //获取想要的字段
            String serverTime = fields[1];
            String platform = fields[13];
            String browserName=fields[24];
            String browserVersion=fields[25];
//            String uuid = fields[3];
            String url=fields[10];

            if ( url.equals("null") || StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(url))
            {
                logger.info("serverTime & url is null.serverTime:" + serverTime + ". url:" + url);
                return;
            }

            //构造输出的key
            PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
            DateDimension dateDimension = DateDimension.buildDate(Long.parseLong(serverTime), DateEnum.DAY);

            StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

            statsCommonDimension.setDateDimension(dateDimension);
            statsCommonDimension.setPlatformDimension(platformDimension);
            statsCommonDimension.setKpiDimension(pvKpi);


            //创建浏览器模式

            BrowserDimension browserDimension=new BrowserDimension(browserName,browserVersion);
            this.k.setBrowserDimension(browserDimension);

            this.v.setId(url);

            context.write(k,v);
        }

    }
}