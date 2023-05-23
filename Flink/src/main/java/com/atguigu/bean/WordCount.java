package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * POJO：等价Java Bean的封装对象
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WordCount {
    private String word;
    private Integer count;
}
