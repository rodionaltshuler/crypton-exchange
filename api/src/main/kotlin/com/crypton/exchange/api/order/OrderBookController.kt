package com.crypton.exchange.api.order

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.ksql.api.client.Client
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class OrderBookController(private val ksql: Client, private val mapper: ObjectMapper) {

    fun markets() {

    }

    fun ticker() {

    }

    //volume available at each price -> L3 grouped by price
    fun orderBookL2(){

    }

    @GetMapping(path = ["/order-book/l3"])
    fun orderBookL3(): OrderBookDto {
        val query = "SELECT * FROM QUERYABLE_ORDERS;"
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

        return OrderBookDto(orderKsqls)
    }


}

data class OrderBookDto(val orderKsqls: List<OrderKsql>)
