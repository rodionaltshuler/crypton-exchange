package com.crypton.exchange.api.order

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.ksql.api.client.Client
import org.apache.kafka.clients.producer.ProducerRecord
import com.crypton.exchange.domain.HasOrderId
import com.crypton.exchange.domain.OrderCommand
import com.crypton.exchange.domain.OrderCommandType
import org.springframework.http.HttpStatus
import org.springframework.http.codec.ServerSentEvent
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
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
    fun orderUpdates(@PathVariable("order_id") orderId: String): Flux<ServerSentEvent<HasOrderId>> {

        val handle = orderStatusPublisher.subscribe(orderId)

        val existingOrder: Flux<HasOrderId> = Flux.from(orderStatus(orderId));
        val updates = orderStatusPublisher.listen(orderId)

        //return Flux.concat(existingOrder, updates)
        return existingOrder.concatWith(updates)
            .map {
                ServerSentEvent.builder<HasOrderId>()
                    .id("order/${orderId}/${System.currentTimeMillis()}")
                    .data(it)
                    .build()
            } .doAfterTerminate {
                orderStatusPublisher.unsubscribe(handle)
            }
    }

    private fun orderStatus(orderId: String): Mono<OrderKsql> {
        val query = "SELECT * FROM QUERYABLE_ORDERS WHERE ID = '${orderId}';"
        val rows = ksql.executeQuery(query).get()

        val ordersKsql = rows.map {
            val columns = it.columnNames()
            val values = it.values()
            val map = HashMap<String, Any>()
            columns.indices.forEach { i ->
                map[columns[i]] = values.getValue(i)
            }
            mapper.convertValue(map, OrderKsql::class.java)
        }

        return when (ordersKsql.isEmpty()) {
            true -> Mono.empty()
            false -> Mono.just(ordersKsql.first())
        }
    }
}