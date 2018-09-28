package com.phone.analystic.mr.activeUser;

import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.common.DateEnum;
import com.phone.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * FileName: NewUserReducer
 * Author: zhao
 * Date: 2018/9/20 20:39
 * Description:统计活跃用户和各时间段的活跃用户
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class ActiveUserReducer extends Reducer<StatsUserDimension, TimeOutputValue,
        StatsUserDimension, OutputWritable> {
    private static final Logger logger = Logger.getLogger(ActiveUserReducer.class);
    private OutputWritable v = new OutputWritable();
    private Set unique = new HashSet();  //用于去重uuid
    private MapWritable map = new MapWritable();  //用于输出

    //小时统计
    private Map<Integer, Set<String>> hourlyMap = new HashMap<Integer, Set<String>>();
    private MapWritable houlyWritable = new MapWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        for (int i = 0; i < 24; i++) {
            hourlyMap.put(i, new HashSet<String>());
            houlyWritable.put(new IntWritable(i), new IntWritable(0));
        }
    }

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空map
        map.clear();
        unique.clear();
        //循环
        for (TimeOutputValue tv : values) {
            //活跃用户uuid过滤
            this.unique.add(tv.getId());
            //各小时的活跃用户
            if (key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_USERS.kpiName)) {
                int hour = TimeUtil.getDateInfo(tv.getTime(), DateEnum.HOUR);
                hourlyMap.get(hour).add(tv.getId());
            }
        }

        //各小时活跃用户的统计 hourly表
        if (key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.ACTIVE_USERS.kpiName)) {

            for(Map.Entry<Integer, Set<String>> en :hourlyMap.entrySet()){
                houlyWritable.put(new IntWritable(en.getKey()),new IntWritable(en.getValue().size()));
            }

            this.v.setValue(houlyWritable);
            this.v.setKpi(KpiType.HOURLY_ACTIVE_USER);

            context.write(key,v);
        }


        //构造输出的v
        v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        v.setValue(map);

        context.write(key, v);


        this.hourlyMap.clear();
        this.houlyWritable.clear();
        for(int i = 0; i < 24 ; i++){
            this.hourlyMap.put(i,new HashSet<String>());
            this.houlyWritable.put(new IntWritable(i),new IntWritable(0));
        }
    }
}
