package com.phone.analystic.mr.pv;

import com.phone.analystic.modle.StatsBaseDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.StatsOutpuValue;
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
 * Description:将新用户信息写入mysql
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class PvOutputWriter implements IOutputWritter {
    private static  Logger logger= Logger.getLogger(PvOutputWriter.class);

    @Override
    public void ouput(Configuration conf, StatsBaseDimension key, StatsOutpuValue value, PreparedStatement ps, IDimension iDimension) {


        try {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;
            //pv赋值
            int pv = ((IntWritable) (v.getValue().get(new IntWritable(-1)))).get();

            int i = 0;
            ps.setInt(++i,iDimension.getDiemnsionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDiemnsionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            ps.setInt(++i,iDimension.getDiemnsionIdByObject(k.getBrowserDimension()));
            ps.setInt(++i,pv);
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i,pv);

            ps.addBatch(); //添加到批处理中
        } catch (Exception e) {
            logger.error("浏览器新用户指标ps赋值错误",e);
        }

    }
}
