package com.crypton.exchange.api

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.confluent.ksql.api.client.Client
import io.confluent.ksql.api.client.ClientOptions
import org.example.domain.OrderCommand
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonSerializer


@Configuration
class AppConfiguration(@Value("\${spring.kafka.bootstrap-servers}") private val bootstrapServers: String,
                       @Value("\${ksqldb.host}") private val host: String,
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

    @Bean
    fun producerFactory() = DefaultKafkaProducerFactory<String, OrderCommand>(producerConfigs())

    @Bean
    fun producerConfigs() = mapOf<String, Any>(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java
    )

    @Bean
    fun kafkaTemplate(): KafkaTemplate<String, OrderCommand> = KafkaTemplate(producerFactory())

}