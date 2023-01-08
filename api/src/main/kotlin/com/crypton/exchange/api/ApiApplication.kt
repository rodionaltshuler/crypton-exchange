package com.crypton.exchange.api

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.confluent.ksql.api.client.Client
import io.confluent.ksql.api.client.ClientOptions
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@SpringBootApplication
class ApiApplication

fun main(args: Array<String>) {
    runApplication<ApiApplication>(*args)
}
