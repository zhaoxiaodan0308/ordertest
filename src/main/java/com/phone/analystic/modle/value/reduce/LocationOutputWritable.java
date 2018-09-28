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
public class LocationOutputWritable extends StatsOutpuValue {
    private KpiType kpi;
    private int aus;
    private int sessions;
    private int bounce_sessions;

    public LocationOutputWritable() {
    }

    public LocationOutputWritable(KpiType kpi, int aus, int sessions, int bounce_sessions) {
        this.kpi = kpi;
        this.aus = aus;
        this.sessions = sessions;
        this.bounce_sessions = bounce_sessions;
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        WritableUtils.writeEnum(dataOutput, kpi);
        dataOutput.writeInt(aus);
        dataOutput.writeInt(this.sessions);
        dataOutput.writeInt(this.bounce_sessions);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        WritableUtils.readEnum(dataInput, KpiType.class);
        this.aus = dataInput.readInt();
        this.sessions = dataInput.readInt();
        this.bounce_sessions = dataInput.readInt();
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public int getAus() {
        return aus;
    }

    public void setAus(int aus) {
        this.aus = aus;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getBounce_sessions() {
        return bounce_sessions;
    }

    public void setBounce_sessions(int bounce_sessions) {
        this.bounce_sessions = bounce_sessions;
    }
}
