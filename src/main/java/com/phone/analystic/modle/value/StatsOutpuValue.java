package com.phone.analystic.modle.value;

import com.phone.common.KpiType;
import org.apache.hadoop.io.Writable;

/**
 * FileName: StatsOutpuValue
 * Author: zhao
 * Date: 2018/9/20 18:52
 * Description:封装map或者是reduce阶段的输出value的类型的顶级父类
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public abstract  class StatsOutpuValue implements Writable {
    //获取kpi的抽象方法
    public abstract KpiType getKpi();
}
