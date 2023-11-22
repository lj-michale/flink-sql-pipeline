package org.turing.java.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @descri:  车速日志
 *
 * @author: lj.michale
 * @date: 2023/11/10 15:04
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class CarLog {
    private String carCode;   //车编号
    private String vin;     //车架号
    private int speed;      //车速：km/h
    private long logTime;   //数据记录时间：ms
    private String gpsLongitude; //GPS经度
    private String gpsLatitude; //GPS维度
}
