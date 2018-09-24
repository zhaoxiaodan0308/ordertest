package com.phone.analystic.modle.value.map;

import com.phone.analystic.modle.value.StatsOutpuValue;
import com.phone.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FileName: TimeOutputValue
 * Author: zhao
 * Date: 2018/9/20 20:01
 * Description:用于map阶段输出的value的类型
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class TimeOutputValue extends StatsOutpuValue {
    private String id; //对id的泛指，可以是uuid，可以是umid，可以是sessionId
    private long time; //时间戳

    public TimeOutputValue() {
    }

    public TimeOutputValue(String id, long time) {
        this.id = id;
        this.time = time;
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeLong(time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.time=dataInput.readLong();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
