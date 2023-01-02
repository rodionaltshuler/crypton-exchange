package org.example

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.TestRecord
import org.example.match.properties
import org.example.match.topology
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.JsonSerde

class ApplicationTests {

    private lateinit var testDriver: TopologyTestDriver

    private val objectMapper = jacksonObjectMapper()
    @BeforeEach
    fun setup() {
        val topology = topology()
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
    fun `should reject cancellation of non-existing orders`() {
        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command-to-reject.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val record = TestRecord(orderCommand.orderId, orderCommand)

        input.pipeInput(record)

        val outputTopic = testDriver.createOutputTopic("order-commands-rejected", Serdes.String().deserializer(), JsonSerde(OrderCommand::class.java).deserializer())
        val actualOutputRecord = outputTopic.readKeyValue()

        assert(actualOutputRecord.value.order.status == OrderStatus.REJECTED)
    }

    @Test
    fun `rejected order doesn't produce wallet command`() {
        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command-to-reject.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val record = TestRecord(orderCommand.orderId, orderCommand)

        input.pipeInput(record)

        val outputTopic = testDriver.createOutputTopic("wallet-commands", Serdes.String().deserializer(), JsonSerde(WalletCommand::class.java).deserializer())
        assert(outputTopic.isEmpty)
    }

    @Test
    fun `new accepted order generates wallet command`() {
        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val record = TestRecord(orderCommand.orderId, orderCommand)

        input.pipeInput(record)

        val outputTopic = testDriver.createOutputTopic("wallet-commands", Serdes.String().deserializer(), JsonSerde(WalletCommand::class.java).deserializer())
        val actualOutputRecord = outputTopic.readRecord()


        assert(actualOutputRecord.value.causeId == orderCommand.orderId)
        assert(actualOutputRecord.value.operation == WalletOperation.BLOCK)

    }

    @Test
    fun `when order is accepted wallet-commands topic is not empty, and rejected topic is empty`() {
        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val record = TestRecord(orderCommand.orderId, orderCommand)

        input.pipeInput(record)

        val outputTopic = testDriver.createOutputTopic("wallet-commands", Serdes.String().deserializer(), JsonSerde(WalletCommand::class.java).deserializer())
        assert(!outputTopic.isEmpty)


        val outputTopicRejected = testDriver.createOutputTopic("order-commands-rejected", Serdes.String().deserializer(), JsonSerde(OrderCommand::class.java).deserializer())
        assert(outputTopicRejected.isEmpty)

    }
}