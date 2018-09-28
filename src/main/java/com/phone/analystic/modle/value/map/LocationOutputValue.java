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
public class LocationOutputValue extends StatsOutpuValue {
    private String uid; //uuid
    private String sid; //sessionID

    public LocationOutputValue() {
    }

    public LocationOutputValue(String uid, String sid) {
        this.uid = uid;
        this.sid = sid;
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(uid);
        dataOutput.writeUTF(sid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uid = dataInput.readUTF();
        this.sid = dataInput.readUTF();
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    @Override
    public String toString() {
        return "LocationOutputValue{" +
                "uid='" + uid + '\'' +
                ", sid='" + sid + '\'' +
                '}';
    }
}
