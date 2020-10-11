package com.rocketmq.order;

import com.sun.org.apache.xpath.internal.operations.Or;

import java.util.ArrayList;
import java.util.List;

public class OrderStep {

    private long orderId;

    private String description;

    public long getOrderId() {
        return orderId;
    }

    public void setOrderId(long orderId) {
        this.orderId = orderId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return "OrderStep{" +
                "orderId=" + orderId +
                ", description='" + description + '\'' +
                '}';
    }

    public static List<OrderStep> buildOrders(){
        // 1039: 创建 付款 推送 完成
        // 1065：创建 付款 完成
        // 7235：创建 付款 完成
        List<OrderStep> orders = new ArrayList<>();

        OrderStep order = new OrderStep();
        order.setOrderId(1039);
        order.setDescription("创建");
        orders.add(order);

        order = new OrderStep();
        order.setOrderId(1065);
        order.setDescription("创建");
        orders.add(order);

        order = new OrderStep();
        order.setOrderId(7235);
        order.setDescription("创建");
        orders.add(order);

        order = new OrderStep();
        order.setOrderId(1039);
        order.setDescription("付款");
        orders.add(order);

        order = new OrderStep();
        order.setOrderId(1065);
        order.setDescription("付款");
        orders.add(order);

        order = new OrderStep();
        order.setOrderId(7235);
        order.setDescription("付款");
        orders.add(order);

        order = new OrderStep();
        order.setOrderId(1065);
        order.setDescription("完成");
        orders.add(order);

        order = new OrderStep();
        order.setOrderId(1039);
        order.setDescription("推送");
        orders.add(order);

        order = new OrderStep();
        order.setOrderId(7235);
        order.setDescription("完成");
        orders.add(order);

        order = new OrderStep();
        order.setOrderId(1039);
        order.setDescription("完成");
        orders.add(order);

        return orders;
    }
}
