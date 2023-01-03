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

    @Test
    fun `wallet command rejected comes to wallet-commands-rejected topic only and has REJECTED status`() {
        val input = testDriver.createInputTopic("wallet-commands", Serdes.String().serializer(), JsonSerde(WalletCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("wallet-command-block.json")?.bufferedReader()?.readText()
        val walletCommand = objectMapper.readValue(inputStream, WalletCommand::class.java)
        val record = TestRecord(walletCommand.walletId, walletCommand)

        input.pipeInput(record)

        val outputTopicRejected = testDriver.createOutputTopic("wallet-commands-rejected", Serdes.String().deserializer(), JsonSerde(WalletCommand::class.java).deserializer())
        assert(!outputTopicRejected.isEmpty)

        val outputRecord = outputTopicRejected.readRecord()
        assert(outputRecord.value.status == WalletCommandStatus.REJECTED)

        val outputTopicConfirmed = testDriver.createOutputTopic("wallet-commands-confirmed", Serdes.String().deserializer(), JsonSerde(WalletCommand::class.java).deserializer())
        assert(outputTopicConfirmed.isEmpty)


    }

    @Test
    fun `wallet command confirmed comes to wallet-commands-confirmed topic only and has CONFIRMED status`() {
        val input = testDriver.createInputTopic("wallet-commands", Serdes.String().serializer(), JsonSerde(WalletCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("wallet-command-credit.json")?.bufferedReader()?.readText()
        val walletCommand = objectMapper.readValue(inputStream, WalletCommand::class.java)
        val record = TestRecord(walletCommand.walletId, walletCommand)

        input.pipeInput(record)

        val outputTopicRejected = testDriver.createOutputTopic("wallet-commands-rejected", Serdes.String().deserializer(), JsonSerde(WalletCommand::class.java).deserializer())
        assert(outputTopicRejected.isEmpty)


        val outputTopicConfirmed = testDriver.createOutputTopic("wallet-commands-confirmed", Serdes.String().deserializer(), JsonSerde(WalletCommand::class.java).deserializer())
        assert(!outputTopicConfirmed.isEmpty)

        val outputRecord = outputTopicConfirmed.readRecord()
        assert(outputRecord.value.status == WalletCommandStatus.CONFIRMED)

    }

    @Test
    fun `confirmed wallet command caused by order has orderId header`() {


        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val record = TestRecord(orderCommand.orderId, orderCommand)

        val walletStore = testDriver.getKeyValueStore<String, Wallet>("wallet-store")
        var wallet= Wallet(orderCommand.order.walletId, emptyMap())
        val assets : Map<String, Asset> = wallet.assets + arrayOf(
            orderCommand.order.baseAssetId to Asset(orderCommand.order.baseAssetId, orderCommand.order.qty * orderCommand.order.price, 0.0),
            orderCommand.order.quoteAssetId to Asset(orderCommand.order.quoteAssetId, orderCommand.order.qty * orderCommand.order.price, 0.0)
        )
        wallet = wallet.copy(assets = assets)
        walletStore.put(wallet.walletId, wallet)


        input.pipeInput(record)

        val outputTopic = testDriver.createOutputTopic("wallet-commands-confirmed", Serdes.String().deserializer(), JsonSerde(OrderCommand::class.java).deserializer())
        val actualOutputRecord = outputTopic.readRecord()

        val header = actualOutputRecord.headers().find { it.key() == "orderId" }

        assert(header != null)
        assert(String(header!!.value()) == orderCommand.orderId)

    }
}