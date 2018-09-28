package com.phone.analystic.mr.nm;

import com.phone.Util.JdbcUtil;
import com.phone.Util.MemberUtil;
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
import java.sql.Connection;

/**
 * 用户模块下的新增用户
 */
public class NewMembersMapper extends Mapper<LongWritable, Text, StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewMembersMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();

    private KpiDimension newMembersKpi = new KpiDimension(KpiType.NEW_MEMBERS.kpiName);
    private KpiDimension browserNewMembersKpi = new KpiDimension(KpiType.BROWSER_NEW_MEMBERS.kpiName);

    private Connection conn=null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //删除当前天新增的用户
        MemberUtil.deleteByDay(context.getConfiguration());
        conn= JdbcUtil.getConn();
    }

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
        String memberId = fields[4];

        if (memberId.equals("null") || StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(memberId)) {
            logger.info("serverTime & uuid is null.serverTime:" + serverTime + ". memberId:" + memberId);

            return;
        }

        //判断是不是为新增会员 true:新会员  false：老会员
        if(!MemberUtil.isNewMember(memberId,conn,context.getConfiguration())){
            logger.info("该会员是一个老会员.memberId:"+memberId);
            return;
        }

        //构造输出的key
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(Long.parseLong(serverTime), DateEnum.DAY);

        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setKpiDimension(newMembersKpi);

        //设置默认的浏览器对象
        BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");
        this.k.setBrowserDimension(defaultBrowserDimension);
        this.k.setStatsCommonDimension(statsCommonDimension);

        //构建输出的value
        this.v.setId(memberId);
        this.v.setTime(Long.parseLong(serverTime));

        context.write(k, v);

        //创建浏览器模式
        String browserName = fields[24];
        String browserVersion = fields[25];
        BrowserDimension browserDimension = new BrowserDimension(browserName, browserVersion);
        this.k.getStatsCommonDimension().setKpiDimension(browserNewMembersKpi);
        this.k.setBrowserDimension(browserDimension);

        context.write(k, v);
    }

}