package com.phone.analystic.modle.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FileName: CurrencyTypeDimension
 * Author: zhao
 * Date: 2018/10/4 17:07
 * Description:支付货币类型
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class CurrencyTypeDimension extends BaseDimension {
    private int id;
    private String currency_name;

    public CurrencyTypeDimension() {
    }

    public CurrencyTypeDimension(String currency_name) {
        this.currency_name = currency_name;
    }

    public CurrencyTypeDimension(int id, String currency_name) {
        this.id = id;
        this.currency_name = currency_name;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }

        CurrencyTypeDimension other = (CurrencyTypeDimension) o;

        int temp = this.id - other.id;
        if (temp != 0) {
            return temp;
        }

        return this.currency_name.compareTo(other.currency_name);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(currency_name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.currency_name = dataInput.readUTF();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrency_name() {
        return currency_name;
    }

    public void setCurrency_name(String currency_name) {
        this.currency_name = currency_name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CurrencyTypeDimension that = (CurrencyTypeDimension) o;

        if (id != that.id) return false;
        return currency_name != null ? currency_name.equals(that.currency_name) : that.currency_name == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (currency_name != null ? currency_name.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "CurrencyTypeDimension{" +
                "id=" + id +
                ", currency_name='" + currency_name + '\'' +
                '}';
    }
}
