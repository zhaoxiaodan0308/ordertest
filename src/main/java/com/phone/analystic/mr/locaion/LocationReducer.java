package com.phone.analystic.mr.locaion;

import com.phone.analystic.modle.StatsLocationDimension;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.map.LocationOutputValue;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.LocationOutputWritable;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.common.KpiType;
import org.apache.commons.lang.StringUtils;
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
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class LocationReducer extends Reducer<StatsLocationDimension, LocationOutputValue,
        StatsLocationDimension, LocationOutputWritable> {
    private static final Logger logger = Logger.getLogger(LocationReducer.class);
    private LocationOutputWritable v = new LocationOutputWritable();
    private Set unique = new HashSet();  //用于去重uuid
    private Map<String, Integer> map = new HashMap<String, Integer>(); //sessionID

    @Override
    protected void reduce(StatsLocationDimension key, Iterable<LocationOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空map
        unique.clear();
        //循环
        for (LocationOutputValue tv : values) {
            //uuid去重
            if (StringUtils.isNotEmpty(tv.getUid())) {
                this.unique.add(tv.getUid());
            }

            //获取会话
            if (StringUtils.isNotEmpty(tv.getSid())) {
                if (map.containsKey(tv.getSid())) {
                    map.put(tv.getSid(), 2);
                } else {
                    map.put(tv.getSid(), 1);
                }
            }
        }


        //构造输出的v
        v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        v.setAus(unique.size());
        v.setSessions(map.size());

        int i = 0;
        for (Map.Entry<String, Integer> en : map.entrySet()) {
            if (en.getValue() == 1) {
                i++;
            }
        }
        v.setBounce_sessions(i);

        context.write(key, v);
    }
}
