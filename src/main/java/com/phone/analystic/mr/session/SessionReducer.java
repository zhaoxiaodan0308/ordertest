package com.phone.analystic.mr.session;

import com.phone.Util.TimeUtil;
import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.common.DateEnum;
import com.phone.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

/**
 * FileName: NewUserReducer
 * Author: zhao
 * Date: 2018/9/20 20:39
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class SessionReducer extends Reducer<StatsUserDimension, TimeOutputValue,
        StatsUserDimension, OutputWritable> {
    private static final Logger logger = Logger.getLogger(SessionReducer.class);
    private OutputWritable v = new OutputWritable();
    private MapWritable map = new MapWritable();  //用于输出
    private Map<String, List<Long>> li = new HashMap<String, List<Long>>();  //存放sessionID信息

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

        for (TimeOutputValue va : values) {
            //将所有的sessionID信息保存
            if (li.containsKey(va.getId())) {
                li.get(va.getId()).add(va.getTime());
            } else {
                List<Long> list = new ArrayList<>();
                list.add(va.getTime());
                li.put(va.getId(), list);
            }

            //各小时会话个数的统计
            if (key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.SESSION.kpiName)) {
                int hour = TimeUtil.getDateInfo(va.getTime(), DateEnum.HOUR);
                hourlyMap.get(hour).add(va.getId());
            }
        }

        //各小时会话个数的统计 hourly表
        if (key.getStatsCommonDimension().getKpiDimension().getKpiName().equals(KpiType.SESSION.kpiName)) {

            for(Map.Entry<Integer, Set<String>> en :hourlyMap.entrySet()){
                houlyWritable.put(new IntWritable(en.getKey()),new IntWritable(en.getValue().size()));
            }

            this.v.setValue(houlyWritable);
            this.v.setKpi(KpiType.HOURLY_SESSION);

            context.write(key,v);
        }

        //循环获取时长,并过滤sessionid
        int sessionLength = 0;
        for (Map.Entry<String, List<Long>> en : li.entrySet()) {
            if (en.getValue().size() >=2 ) {
                sessionLength += Collections.max(en.getValue()) - Collections.min(en.getValue());
            }
        }

        //将毫秒转成秒，不满1秒的算1秒
        if(sessionLength>0){
            if(sessionLength % 1000 == 0 ){
                sessionLength = sessionLength / 1000;
            }else{
                sessionLength =sessionLength / 1000 +1;
            }
        }


        //构造输出的v
        v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        map.put(new IntWritable(-1), new IntWritable(this.li.size()));
        map.put(new IntWritable(-2), new IntWritable(sessionLength));
        v.setValue(map);

        context.write(key, v);

        li.clear();
    }
}
