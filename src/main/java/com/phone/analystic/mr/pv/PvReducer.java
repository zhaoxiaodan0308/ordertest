package com.phone.analystic.mr.pv;

import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * FileName: NewUserReducer
 * Author: zhao
 * Date: 2018/9/20 20:39
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class PvReducer extends Reducer<StatsUserDimension, TimeOutputValue,
        StatsUserDimension, OutputWritable> {
    private static final Logger logger = Logger.getLogger(PvReducer.class);
    private OutputWritable v = new OutputWritable();
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空map
        map.clear();

        int i=0;
        //循环
        for (TimeOutputValue tv: values) {
           i++;
        }
        //构造输出的v
        v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        map.put(new IntWritable(-1),new IntWritable(i));
        v.setValue(map);

        context.write(key,v);
    }
}
