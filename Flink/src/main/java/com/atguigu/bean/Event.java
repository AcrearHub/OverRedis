package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 事件
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    private String user;
    private String url;
    private Long ts;
}
