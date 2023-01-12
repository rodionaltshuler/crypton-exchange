package com.crypton.exchange.matching

import com.crypton.exchange.events.OrderCommand
import com.crypton.exchange.events.OrdersMatchCommand
import com.crypton.exchange.matching.topology.ordersMatchTopology
import com.crypton.exchange.processing.properties
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.TestRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.JsonSerde

class OrdersMatchOrderProcessingApplicationTests {

    private lateinit var testDriver: TopologyTestDriver

    private val objectMapper = jacksonObjectMapper()
    @BeforeEach
    fun setup() {
        val topology = ordersMatchTopology()
        val props = properties()
        props[StreamsConfig.APPLICATION_ID_CONFIG] = "test"
        props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy:1234"
        testDriver = TopologyTestDriver(topology, props)
    }

    @AfterEach
    fun tearDown() {
        testDriver.close()
    }

    @Test
    fun `OrderMatchCommand generates 2 order commands`() {
        val input = testDriver.createInputTopic("order-match-commands", Serdes.String().serializer(), JsonSerde(
            OrdersMatchCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-match-command.json")?.bufferedReader()?.readText()
        val matchCommand = objectMapper.readValue(inputStream, OrdersMatchCommand::class.java)

        val record = TestRecord(matchCommand.matchId, matchCommand)

        input.pipeInput(record)

        val outputTopic = testDriver.createOutputTopic("order-commands", Serdes.String().deserializer(), JsonSerde(
            OrderCommand::class.java).deserializer())
        assert(!outputTopic.isEmpty)

        val records = outputTopic.readRecordsToList()
        assert(records.size == 2)

    }

}