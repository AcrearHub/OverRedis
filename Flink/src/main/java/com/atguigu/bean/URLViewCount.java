package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor

public class URLViewCount {
    private Long start; //起始时间
    private Long end;   //结束时间
    private String url; //统计页面
    private Long count; //点击次数
}
