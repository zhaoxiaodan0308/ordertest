package com.phone.analystic.modle.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * FileName: PaymentTypeDimension
 * Author: zhao
 * Date: 2018/10/4 17:07
 * Description:支付手段
 * History:
 * <author> <time> <version> <desc>
 * 作者姓名 修改时间 版本号 描述
 */
public class PaymentTypeDimension extends BaseDimension {
    private int id;
    private String payment_type;

    public PaymentTypeDimension() {
    }

    public PaymentTypeDimension(String payment_type) {
        this.payment_type = payment_type;
    }

    public PaymentTypeDimension(int id, String payment_type) {
        this.id = id;
        this.payment_type = payment_type;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }

        PaymentTypeDimension other = (PaymentTypeDimension) o;

        int temp = this.id - other.id;
        if (temp != 0) {
            return temp;
        }

        return this.payment_type.compareTo(other.payment_type);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(payment_type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.payment_type = dataInput.readUTF();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPayment_type() {
        return payment_type;
    }

    public void setPayment_type(String payment_type) {
        this.payment_type = payment_type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PaymentTypeDimension that = (PaymentTypeDimension) o;

        if (id != that.id) return false;
        return payment_type != null ? payment_type.equals(that.payment_type) : that.payment_type == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (payment_type != null ? payment_type.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PaymentTypeDimension{" +
                "id=" + id +
                ", payment_type='" + payment_type + '\'' +
                '}';
    }
}
