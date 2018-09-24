package com.phone.analystic.modle;

import com.phone.analystic.modle.base.BaseDimension;
import com.phone.analystic.modle.base.DateDimension;
import com.phone.analystic.modle.base.KpiDimension;
import com.phone.analystic.modle.base.PlatformDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FileName: StatsCommonDimension
 * Author: zhao
 * Date: 2018/9/20 19:01
 * Description:
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class StatsCommonDimension extends StatsBaseDimension {
    private DateDimension dateDimension = new DateDimension();
    private PlatformDimension platformDimension = new PlatformDimension();
    private KpiDimension kpiDimension = new KpiDimension();

    public StatsCommonDimension() {
    }

    public StatsCommonDimension(DateDimension dateDimension, PlatformDimension platformDimension, KpiDimension kpiDimension) {
        this.dateDimension = dateDimension;
        this.platformDimension = platformDimension;
        this.kpiDimension = kpiDimension;
    }

    @Override
    public int compareTo(BaseDimension o) {
        StatsCommonDimension other = (StatsCommonDimension) o;
        int temp = dateDimension.compareTo(other.getDateDimension());
        if (temp != 0) {
            return temp;
        }

        temp = platformDimension.compareTo(other.getPlatformDimension());
        if (temp != 0) {
            return temp;
        }

        return kpiDimension.compareTo(other.getKpiDimension());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.dateDimension.write(dataOutput);
        this.platformDimension.write(dataOutput);
        this.kpiDimension.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.dateDimension.readFields(dataInput);
        this.platformDimension.readFields(dataInput);
        this.kpiDimension.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof StatsCommonDimension)) return false;

        StatsCommonDimension that = (StatsCommonDimension) o;

        if (dateDimension != null ? !dateDimension.equals(that.dateDimension) : that.dateDimension != null)
            return false;
        if (platformDimension != null ? !platformDimension.equals(that.platformDimension) : that.platformDimension != null)
            return false;
        return kpiDimension != null ? kpiDimension.equals(that.kpiDimension) : that.kpiDimension == null;
    }

    @Override
    public int hashCode() {
        int result = dateDimension != null ? dateDimension.hashCode() : 0;
        result = 31 * result + (platformDimension != null ? platformDimension.hashCode() : 0);
        result = 31 * result + (kpiDimension != null ? kpiDimension.hashCode() : 0);
        return result;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public PlatformDimension getPlatformDimension() {
        return platformDimension;
    }

    public void setPlatformDimension(PlatformDimension platformDimension) {
        this.platformDimension = platformDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }
}
