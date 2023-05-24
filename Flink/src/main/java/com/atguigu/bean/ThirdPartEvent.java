package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ThirdPartEvent {
    private String orderId; //订单id
    private String eventType;   //事件类型：pay
    private String thirdPartName;   //支付平台名字：AliPay、WeChat
    private Long ts;
}
