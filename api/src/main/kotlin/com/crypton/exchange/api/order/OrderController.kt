package com.crypton.exchange.api.order

import org.example.domain.OrderCommand
import org.apache.kafka.clients.producer.ProducerRecord
import org.example.domain.HasOrderId
import org.example.domain.OrderCommandType
import org.springframework.http.codec.ServerSentEvent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Flux
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.ws.rs.PathParam

@RestController()
class OrderController(
    private val orderStatusPublisher: OrderStatusPublisher,
    private val kafkaTemplate: KafkaTemplate<String, OrderCommand>
) {

    private val executor = Executors.newScheduledThreadPool(4)

    @PostMapping("/order/submit")
    fun submitOrder(@RequestBody command: OrderCommand): Flux<ServerSentEvent<HasOrderId>> {

        val orderId = UUID.randomUUID().toString()
        val orderCommandId = UUID.randomUUID().toString()

        val order = command.order!!.copy(id = orderId)

        val commandToSend = command.copy(
            id = orderCommandId,
            orderId = orderId,
            causeId = orderId,
            order = order
        )

        val handle = orderStatusPublisher.subscribe(orderId)

        return orderStatusPublisher.listen(orderId)
            .map {
                ServerSentEvent.builder<HasOrderId>()
                    .id("order/${commandToSend.orderId}/${System.currentTimeMillis()}")
                    .data(it)
                    .build()
            }
            .doOnSubscribe {
                val record = ProducerRecord("order-commands", commandToSend.orderId, commandToSend)
                kafkaTemplate.send(record)
            }
            .doAfterTerminate {
                orderStatusPublisher.unsubscribe(handle)
            }

    }

    @PostMapping("/order/cancel/{order_id}")
    fun cancelOrder(@PathVariable("order_id") orderId: String): Flux<ServerSentEvent<HasOrderId>> {

        val orderCommandId = UUID.randomUUID().toString()

        val commandToSend = OrderCommand(
            id = orderCommandId,
            orderId = orderId,
            causeId = orderId,
            command = OrderCommandType.CANCEL,
            order = null
        )

        val handle = orderStatusPublisher.subscribe(orderId)

        return orderStatusPublisher.listen(orderId)
            .map {
                ServerSentEvent.builder<HasOrderId>()
                    .id("order/${commandToSend.orderId}/${System.currentTimeMillis()}")
                    .data(it)
                    .build()
            }
            .doOnSubscribe {
                val record = ProducerRecord("order-commands", commandToSend.orderId, commandToSend)
                kafkaTemplate.send(record)
            }
            .doAfterTerminate {
                orderStatusPublisher.unsubscribe(handle)
            }
    }

    fun orderStatus(orderId: String) {
        //flux = subscribe to Publisher listening to the topic [order-commands, order-commands-rejected, orders-confirmed]
        //return flux
    }
}