package com.pikaqiu.springboot.conusmer;

import java.util.Map;

import com.pikaqiu.springboot.entity.Order;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitHandler;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.rabbitmq.client.Channel;

/**
 * 消息消费
 */

@Component
public class RabbitReceiver {

    /**
     * 指定监听 @RabbitListener
     * 作为rabbit消息监听方法   @RabbitHandler
     * @param message
     * @param channel
     * @throws Exception
     */

    @RabbitListener(bindings = @QueueBinding(
            //指定queue
            value = @Queue(value = "queue-1", durable = "true"),
            //指定exchange
            exchange = @Exchange(value = "exchange-1",
                    //持久化
                    durable = "true",
                    //消息类型
                    type = "topic",
                    ignoreDeclarationExceptions = "true"),
            //设置路由key
            key = "springboot.*"
    )
    )
    /**
     *重回队列不建议 或者可以设置重回一次
     * 或者根据日志进行处理
     */
    @RabbitHandler
    public void onMessage(Message message, Channel channel) throws Exception {
        System.err.println("--------------------------------------");
        System.err.println("消费端Payload: " + message.getPayload());
        //获取签收标识deliveryTag
        Long deliveryTag = (Long) message.getHeaders().get(AmqpHeaders.DELIVERY_TAG);
        //手工ACK（签收）       标识     是否批量  是否重回队列
        //ack false
        channel.basicNack(deliveryTag, false,false);
    }


    /**
     * spring.rabbitmq.listener.order.queue.name=queue-2
     * spring.rabbitmq.listener.order.queue.durable=true
     * spring.rabbitmq.listener.order.exchange.name=exchange-1
     * spring.rabbitmq.listener.order.exchange.durable=true
     * spring.rabbitmq.listener.order.exchange.type=topic
     * spring.rabbitmq.listener.order.exchange.ignoreDeclarationExceptions=true
     * spring.rabbitmq.listener.order.key=springboot.*
     *
     * @param order
     * @param channel
     * @param headers
     * @throws Exception
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(value = "${spring.rabbitmq.listener.order.queue.name}",
                    durable = "${spring.rabbitmq.listener.order.queue.durable}"),
            exchange = @Exchange(value = "${spring.rabbitmq.listener.order.exchange.name}",
                    durable = "${spring.rabbitmq.listener.order.exchange.durable}",
                    type = "${spring.rabbitmq.listener.order.exchange.type}",
                    ignoreDeclarationExceptions = "${spring.rabbitmq.listener.order.exchange.ignoreDeclarationExceptions}"),
            key = "${spring.rabbitmq.listener.order.key}"
    )
    )
    @RabbitHandler
    public void onOrderMessage(@Payload Order order,//@Payload获取message中的Payload转换成对象
                               Channel channel,
                               @Headers Map<String, Object> headers) throws Exception {
        System.err.println("--------------------------------------");
        System.err.println("消费端order: " + order.getId());
        Long deliveryTag = (Long) headers.get(AmqpHeaders.DELIVERY_TAG);
        /**
         * ack就直接消费了
         * nack requeue为true就会一直推送
         * nack requeue为false就ack是一样
         * 不ack 就会堆积在broke中 重新启动服务会被重新推送一次
         *
         * 建议不重回队列也不消息堆积
         * 因为一般来时是业务问题 打日志处理
         */

        //手工ACK 重回队列  会被重新推送 如要需要重回建议设置重回次数
        //会把消息重新添加到队列的尾部 重新推送
        //channel.basicNack(deliveryTag, false,false);
        //channel.basicAck(deliveryTag, false);

        System.out.println(deliveryTag);
    }


}
