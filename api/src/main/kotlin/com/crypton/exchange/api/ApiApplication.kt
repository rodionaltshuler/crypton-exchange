package com.crypton.exchange.api

import com.fasterxml.jackson.annotation.JsonAlias
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.confluent.ksql.api.client.Client
import io.confluent.ksql.api.client.ClientOptions
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@SpringBootApplication
class ApiApplication

fun main(args: Array<String>) {
    runApplication<ApiApplication>(*args)
}

@Configuration
class KsqlConfiguration(@Value("\${ksqldb.host}") private val host: String,
                        @Value("\${ksqldb.port}") private val port: Int) {

    @Bean
    fun client(): Client {
        val clientOptions = ClientOptions.create()
            .setHost(host)
            .setPort(port)

        return Client.create(clientOptions)
    }

    @Bean
    fun objectMapper() = jacksonObjectMapper()

}

@RestController
class OrderBookController(private val ksql: Client, private val mapper: ObjectMapper) {

    @GetMapping(path = ["/order-book/"])
    fun orderBook(): OrderBookDto {
        val query = "SELECT * FROM QUERYABLE_ORDERS;"
        val rows = ksql.executeQuery(query).get()

        val orders = rows.map {
            val columns = it.columnNames()
            val values = it.values()
            val map = HashMap<String, Any>()
            columns.indices.forEach { i ->
                map[columns[i]] = values.getValue(i)
            }
            mapper.convertValue(map, Order::class.java)
        }

        return OrderBookDto(orders)
    }


}


data class Order(
    @JsonAlias("ID") val id: String,
    @JsonAlias("BASEASSETID") val baseAssetId: String,
    @JsonAlias("QUOTEASSETID") val quoteAssetId: String,
    @JsonAlias("WALLETID") val walletId: String,
    @JsonAlias("ORDERTYPE") val orderType: String,
    @JsonAlias("PRICE") val price: Double,
    @JsonAlias("QTY") val qty: Double,
    @JsonAlias("QTYFILLED") val qtyFilled: Double,
    @JsonAlias("STATUS") val status: String
)


data class OrderBookDto(val orders: List<Order>)