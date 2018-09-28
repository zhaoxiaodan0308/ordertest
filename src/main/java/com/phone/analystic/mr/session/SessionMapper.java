package com.phone.analystic.mr.session;

import com.phone.analystic.modle.StatsCommonDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.BrowserDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.base.KpiDimension;
import com.phone.analystic.modle.base.PlatformDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.common.DateEnum;
import com.phone.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * 用户模块下的会话个数和时长
 */
public class SessionMapper extends Mapper<LongWritable, Text, StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(SessionMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();

    private KpiDimension sessionKpi = new KpiDimension(KpiType.SESSION.kpiName);
    private KpiDimension browserSessionKpi = new KpiDimension(KpiType.BROWSER_SESSION.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            return;
        }

        //拆分
        String[] fields = line.split("\u0001");
        String en = fields[2];

        //获取想要的字段
        String serverTime = fields[1];
        String platform = fields[13];
        String sessionid = fields[5];
        String browserName = fields[24];
        String browserVersion = fields[25];

        if (sessionid.equals("null") || StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(sessionid)) {
            logger.info("serverTime & sessionid is null.serverTime:" + serverTime + ". sessionid:" + sessionid);
            return;
        }

        System.out.println(serverTime + " " + platform + " " + sessionid + " " + browserName+" " + browserVersion);
        //构造输出的key
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(Long.parseLong(serverTime), DateEnum.DAY);

        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setKpiDimension(sessionKpi);

        //设置默认的浏览器对象
        BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");
        this.k.setBrowserDimension(defaultBrowserDimension);
        this.k.setStatsCommonDimension(statsCommonDimension);

        //构建输出的value
        this.v.setId(sessionid);
        this.v.setTime(Long.parseLong(serverTime));

        context.write(k, v);

        //创建浏览器模式

        BrowserDimension browserDimension = new BrowserDimension(browserName, browserVersion);
        this.k.getStatsCommonDimension().setKpiDimension(browserSessionKpi);
        this.k.setBrowserDimension(browserDimension);

        context.write(k, v);
    }

}