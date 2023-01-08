package com.crypton.exchange.api

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.confluent.ksql.api.client.Client
import io.confluent.ksql.api.client.ClientOptions
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@Configuration
class AppConfiguration(@Value("\${ksqldb.host}") private val host: String,
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