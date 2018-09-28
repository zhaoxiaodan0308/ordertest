package com.phone.analystic.mr.locaion;

import com.phone.analystic.modle.StatsCommonDimension;
import com.phone.analystic.modle.StatsLocationDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.base.KpiDimension;
import com.phone.analystic.modle.base.LocationDimension;
import com.phone.analystic.modle.base.PlatformDimension;
import com.phone.analystic.modle.value.map.LocationOutputValue;
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
public class LocationMapper extends Mapper<LongWritable, Text, StatsLocationDimension, LocationOutputValue> {
    private static final Logger logger = Logger.getLogger(LocationMapper.class);
    private StatsLocationDimension k = new StatsLocationDimension();
    private LocationOutputValue v = new LocationOutputValue();

    private KpiDimension locationKpi = new KpiDimension(KpiType.LOCATION.kpiName);


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            return;
        }

        //拆分
        String[] fields = line.split("\u0001");
        //获取想要的字段
        String serverTime = fields[1];
        String platform = fields[13];
        String uid = fields[3];
        String sid = fields[5];
        String country = fields[28];
        String province = fields[29];
        String city = fields[30];

        if (StringUtils.isEmpty(serverTime)) {
            logger.info("serverTime is null:" + serverTime);
            return;
        }

        //构造输出的key
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(Long.parseLong(serverTime), DateEnum.DAY);

        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();

        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setKpiDimension(locationKpi);

        //构建key中location
        LocationDimension locationDimension=LocationDimension.getInstance(country,province,city);

        this.k.setLocationDimension(locationDimension);
        this.k.setStatsCommonDimension(statsCommonDimension);

        //构建输出的value
        this.v.setSid(sid);
        this.v.setUid(uid);

        context.write(k, v);
    }
}