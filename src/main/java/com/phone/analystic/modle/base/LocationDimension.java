package com.phone.analystic.modle.base;

import com.phone.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @ClassName BrowserDimension
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 * @Description地域维度类
 **/
public class LocationDimension extends BaseDimension {
    private int id;
    private String country;
    private String province;
    private String city;

    public LocationDimension() {
    }

    public LocationDimension(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public LocationDimension(int id, String country, String province, String city) {
        this(country, province, city);
        this.id = id;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }

        LocationDimension other = (LocationDimension) o;

        int temp = this.id - other.id;
        if (temp != 0) {
            return temp;
        }

        temp = this.country.compareTo(other.country);
        if (temp != 0) {
            return temp;
        }

        temp = this.province.compareTo(other.province);
        if (temp != 0) {
            return temp;
        }

        return this.city.compareTo(other.city);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(country);
        dataOutput.writeUTF(province);
        dataOutput.writeUTF(city);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.country=dataInput.readUTF();
        this.province=dataInput.readUTF();
        this.city=dataInput.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocationDimension that = (LocationDimension) o;

        if (id != that.id) return false;
        if (country != null ? !country.equals(that.country) : that.country != null) return false;
        if (province != null ? !province.equals(that.province) : that.province != null) return false;
        return city != null ? city.equals(that.city) : that.city == null;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (country != null ? country.hashCode() : 0);
        result = 31 * result + (province != null ? province.hashCode() : 0);
        result = 31 * result + (city != null ? city.hashCode() : 0);
        return result;
    }

    /**
     * location实例
     * @return
     */

    public static LocationDimension getInstance(String country, String province, String city){

        if(StringUtils.isEmpty(country)){
            country=province=city= GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(province)){
            province=city= GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(city)){
            city= GlobalConstants.DEFAULT_VALUE;
        }

        return new LocationDimension(country,province,city);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "LocationDimension{" +
                "id=" + id +
                ", country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}