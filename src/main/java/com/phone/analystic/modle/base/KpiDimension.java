package com.phone.analystic.modle.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName KpiDimension
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description kpi的维度
 **/
public class KpiDimension extends BaseDimension{
    private int id;
    private String kpiName;

    public KpiDimension(){}

    public KpiDimension(String kpiName) {
        this.kpiName = kpiName;
    }

    public KpiDimension(int id, String kpiName) {
        this.id = id;
        this.kpiName = kpiName;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeUTF(this.kpiName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.kpiName = dataInput.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this == o){
            return 0;
        }

        KpiDimension other = (KpiDimension) o;
        int tmp = this.id - other.id;
        if(tmp != 0){
            return tmp;
        }
        return this.kpiName.compareTo(other.kpiName);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KpiDimension that = (KpiDimension) o;
        return id == that.id &&
                Objects.equals(kpiName, that.kpiName);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, kpiName);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }
}