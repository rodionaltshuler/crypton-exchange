package com.crypton.exchange.api.order

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.ksql.api.client.Client
import org.apache.kafka.clients.producer.ProducerRecord
import org.example.domain.HasOrderId
import org.example.domain.OrderCommand
import org.example.domain.OrderCommandType
import org.springframework.http.HttpStatus
import org.springframework.http.codec.ServerSentEvent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import reactor.core.publisher.Flux
import java.util.*
import java.util.concurrent.Executors

@RestController()
class OrderController(
    private val orderStatusPublisher: OrderStatusPublisher,
    private val kafkaTemplate: KafkaTemplate<String, OrderCommand>,
    private val ksql: Client,
    private val mapper: ObjectMapper
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

    @GetMapping("/order/{order_id}")
    fun orderStatus(@PathVariable("order_id") orderId: String) : OrderKsql {

        val query = "SELECT * FROM QUERYABLE_ORDERS WHERE ID = '${orderId}';"
        val rows = ksql.executeQuery(query).get()

        val orderKsqls = rows.map {
            val columns = it.columnNames()
            val values = it.values()
            val map = HashMap<String, Any>()
            columns.indices.forEach { i ->
                map[columns[i]] = values.getValue(i)
            }
            mapper.convertValue(map, OrderKsql::class.java)
        }

        if (orderKsqls.isEmpty()) {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "Order $orderId not found")
        } else {
            return orderKsqls.first()!!
        }


    }
}