package com.phone.analystic.mr.nm;

import com.phone.analystic.modle.StatsUserDimension;
import com.phone.analystic.modle.value.map.TimeOutputValue;
import com.phone.analystic.modle.value.reduce.OutputWritable;
import com.phone.common.KpiType;
import com.phone.etl.mr.LogWritable;
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
public class NewMembersReducer extends Reducer<StatsUserDimension, TimeOutputValue,
        StatsUserDimension, OutputWritable> {
    private static final Logger logger = Logger.getLogger(NewMembersReducer.class);
    private OutputWritable v = new OutputWritable();
    private Set unique = new HashSet();  //用于去重memberID
    private MapWritable map = new MapWritable();  //用于输出
    private Map<String, List<Long>> li = new HashMap<String, List<Long>>();  //存放memberInfo信息


    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        //清空map
        map.clear();
        unique.clear();



        for (TimeOutputValue va : values) {
            //循环过滤memberID
            this.unique.add(va.getId());
            //将所有的uuid信息保存
            if (li.containsKey(va.getId())) {
                li.get(va.getId()).add(va.getTime());
            } else {
                List<Long> list = new ArrayList<>();
                list.add(va.getTime());
                li.put(va.getId(), list);
            }
        }

        //循环输出  用于插入到member_info表中
        for (Map.Entry<String, List<Long>> en : li.entrySet()) {

            this.v.setKpi(KpiType.MEMBER_INFO);
            map.put(new IntWritable(-2), new Text(en.getKey()));
            Collections.sort(en.getValue());
            map.put(new IntWritable(-3), new LongWritable(en.getValue().get(0)));
            this.v.setValue(map);
            context.write(key, v);
        }



        //构造输出的v
        v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        map.put(new IntWritable(-1), new IntWritable(this.unique.size()));
        v.setValue(map);

        context.write(key, v);

        li.clear();
    }
}
