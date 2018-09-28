package com.phone.analystic.mr.locaion;

import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.StatsLocationDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.StatsOutpuValue;
import com.phone.analystic.modle.value.reduce.LocationOutputWritable;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.analystic.mr.IOutputWritter;
import com.phone.analystic.mr.service.IDimension;
import com.phone.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;

/**
 * FileName: NewUserOutputWriter
 * Author: zhao
 * Date: 2018/9/24 13:26
 * Description:地域信息写入mysql
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class LocationWriter implements IOutputWritter {
    private static  Logger logger= Logger.getLogger(LocationWriter.class);

    @Override
    public void ouput(Configuration conf, StatsBaseDimension key, StatsOutpuValue value, PreparedStatement ps, IDimension iDimension) {


        try {
            StatsLocationDimension k = (StatsLocationDimension) key;
            LocationOutputWritable v = (LocationOutputWritable) value;

            int i = 0;
            ps.setInt(++i,iDimension.getDiemnsionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDiemnsionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            ps.setInt(++i,iDimension.getDiemnsionIdByObject(k.getLocationDimension()));
            ps.setInt(++i,v.getAus());
            ps.setInt(++i,v.getSessions());
            ps.setInt(++i,v.getBounce_sessions());
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i,v.getAus());
            ps.setInt(++i,v.getSessions());
            ps.setInt(++i,v.getBounce_sessions());

            ps.addBatch(); //添加到批处理中
        } catch (Exception e) {
            logger.error("地域指标ps赋值错误",e);
        }

    }
}
