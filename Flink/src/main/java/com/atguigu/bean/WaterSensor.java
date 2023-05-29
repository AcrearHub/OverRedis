package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id; //水位传感器类型
    private Integer vc ; //水位记录
    private Long ts ;  // 传感器记录时间戳

}
