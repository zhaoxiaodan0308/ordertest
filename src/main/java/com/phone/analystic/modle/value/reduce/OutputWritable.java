package com.phone.analystic.modle.value.reduce;

import com.phone.analystic.modle.value.StatsOutpuValue;
import com.phone.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FileName: OutputWritable
 * Author: zhao
 * Date: 2018/9/20 20:14
 * Description:用于reduce阶段输出的value的类型
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class OutputWritable extends StatsOutpuValue {
    private KpiType kpi;
    private MapWritable value = new MapWritable();

    public OutputWritable() {
    }

    public OutputWritable(KpiType kpi, MapWritable value) {
        this.kpi = kpi;
        this.value = value;
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput, kpi);
        this.value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        WritableUtils.readEnum(dataInput, KpiType.class);
        this.value.readFields(dataInput);
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }
}
