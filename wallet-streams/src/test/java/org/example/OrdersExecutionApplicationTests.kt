package org.example

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.TestRecord
import org.example.domain.*
import org.example.orders_execution.properties
import org.example.orders_execution.ordersExecutionTopology
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.JsonSerde

class OrdersExecutionApplicationTests {

    private lateinit var testDriver: TopologyTestDriver

    private val objectMapper = jacksonObjectMapper()
    @BeforeEach
    fun setup() {
        val topology = ordersExecutionTopology()
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
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command-cancel.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val record = TestRecord(orderCommand.orderId, orderCommand)

        input.pipeInput(record)

        val outputTopic = testDriver.createOutputTopic("order-commands-rejected", Serdes.String().deserializer(), JsonSerde(
            OrderCommand::class.java).deserializer())
        val actualOutputRecord = outputTopic.readKeyValue()

        assert(actualOutputRecord.value.id == orderCommand.id)
    }

    @Test
    fun `new accepted order generates wallet command`() {
        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val record = TestRecord(orderCommand.orderId, orderCommand)

        input.pipeInput(record)

        val outputTopic = testDriver.createOutputTopic("wallet-commands", Serdes.String().deserializer(), JsonSerde(
            WalletCommand::class.java).deserializer())
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

        val outputTopic = testDriver.createOutputTopic("wallet-commands", Serdes.String().deserializer(), JsonSerde(
            WalletCommand::class.java).deserializer())
        assert(!outputTopic.isEmpty)


        val outputTopicRejected = testDriver.createOutputTopic("order-commands-rejected", Serdes.String().deserializer(), JsonSerde(
            OrderCommand::class.java).deserializer())
        assert(outputTopicRejected.isEmpty)

    }

    @Test
    fun `wallet command rejected comes to wallet-commands-rejected topic only`() {
        val input = testDriver.createInputTopic("wallet-commands", Serdes.String().serializer(), JsonSerde(WalletCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("wallet-command-block.json")?.bufferedReader()?.readText()
        val walletCommand = objectMapper.readValue(inputStream, WalletCommand::class.java)
        val record = TestRecord(walletCommand.walletId, walletCommand)

        input.pipeInput(record)

        val outputTopicRejected = testDriver.createOutputTopic("wallet-commands-rejected", Serdes.String().deserializer(), JsonSerde(
            WalletCommand::class.java).deserializer())
        assert(!outputTopicRejected.isEmpty)

        val outputRecord = outputTopicRejected.readRecord()
        assert(outputRecord.value.status == WalletCommandStatus.REJECTED)

        val outputTopicConfirmed = testDriver.createOutputTopic("wallet-commands-confirmed", Serdes.String().deserializer(), JsonSerde(
            WalletCommand::class.java).deserializer())
        assert(outputTopicConfirmed.isEmpty)


    }

    @Test
    fun `wallet command confirmed comes to wallet-commands-confirmed topic only and has CONFIRMED status`() {
        val input = testDriver.createInputTopic("wallet-commands", Serdes.String().serializer(), JsonSerde(WalletCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("wallet-command-credit.json")?.bufferedReader()?.readText()
        val walletCommand = objectMapper.readValue(inputStream, WalletCommand::class.java)
        val record = TestRecord(walletCommand.walletId, walletCommand)

        input.pipeInput(record)

        val outputTopicRejected = testDriver.createOutputTopic("wallet-commands-rejected", Serdes.String().deserializer(), JsonSerde(
            WalletCommand::class.java).deserializer())
        assert(outputTopicRejected.isEmpty)


        val outputTopicConfirmed = testDriver.createOutputTopic("wallet-commands-confirmed", Serdes.String().deserializer(), JsonSerde(
            WalletCommand::class.java).deserializer())
        assert(!outputTopicConfirmed.isEmpty)

        val outputRecord = outputTopicConfirmed.readRecord()
        assert(outputRecord.value.status == WalletCommandStatus.CONFIRMED)

    }

    @Test
    fun `confirmed wallet command caused by order has orderCommandId header`() {


        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val record = TestRecord(orderCommand.orderId, orderCommand)

        val walletStore = testDriver.getKeyValueStore<String, Wallet>("wallet-store")
        var wallet= Wallet(orderCommand.order!!.walletId, emptyMap())
        val order: Order = orderCommand.order!!
        val assets : Map<String, Asset> = wallet.assets + arrayOf(
            order.baseAssetId to Asset(order.baseAssetId, order.qty * order.price, 0.0),
            order.quoteAssetId to Asset(order.quoteAssetId, order.qty * order.price, 0.0)
        )
        wallet = wallet.copy(assets = assets)
        walletStore.put(wallet.walletId, wallet)

        input.pipeInput(record)

        val outputTopic = testDriver.createOutputTopic("wallet-commands-confirmed", Serdes.String().deserializer(), JsonSerde(
            WalletCommand::class.java).deserializer())
        val actualOutputRecord = outputTopic.readRecord()

        val header = actualOutputRecord.headers().find { it.key() == "orderCommandId" }

        assert(header != null)
        assert(String(header!!.value()) == orderCommand.id)

    }


    @Test
    fun `order is present in the order-store after ConfirmedOrderCommandsProcessor, and order command is cleared from order-commands-store `(){

        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val order: Order = orderCommand.order!!
        val record = TestRecord(orderCommand.orderId, orderCommand)

        val walletStore = testDriver.getKeyValueStore<String, Wallet>("wallet-store")
        var wallet= Wallet(orderCommand.order!!.walletId, emptyMap())
        val assets : Map<String, Asset> = wallet.assets + arrayOf(
            order.baseAssetId to Asset(order.baseAssetId, order.qty * order.price, 0.0),
            order.quoteAssetId to Asset(order.quoteAssetId, order.qty * order.price, 0.0)
        )
        wallet = wallet.copy(assets = assets)
        walletStore.put(wallet.walletId, wallet)

        input.pipeInput(record)

        val outputTopic = testDriver.createOutputTopic("orders-confirmed", Serdes.String().deserializer(), JsonSerde(
            Order::class.java).deserializer())
        val actualOutputRecord = outputTopic.readRecord()
        assert(actualOutputRecord.value != null)
        assert(actualOutputRecord.value.status == OrderStatus.CONFIRMED)

        val orderStore = testDriver.getKeyValueStore<String, Order>("order-store")

        assert(orderStore.get(orderCommand.orderId) != null) //and order is present
        assert(orderStore.get(orderCommand.orderId).status == OrderStatus.CONFIRMED)


        val orderCommandsStore = testDriver.getKeyValueStore<String, OrderCommand>("order-commands-store")
        assert(orderCommandsStore.get(orderCommand.id) == null) { "OrderCommands clean-up not implemented "}
    }



    @Test
    fun `order-command generates order-command-rejected or orders-confirmed`() {

        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)

        val record = TestRecord(orderCommand.order!!.id, orderCommand)

        input.pipeInput(record)

        val outputTopicRejected = testDriver.createOutputTopic("order-commands-rejected", Serdes.String().deserializer(), JsonSerde(
            OrderCommand::class.java).deserializer())
        val outputTopicConfirmed = testDriver.createOutputTopic("orders-confirmed", Serdes.String().deserializer(), JsonSerde(
            Order::class.java).deserializer())

        val recordsRejected = outputTopicRejected.readRecordsToList()
        val recordsConfirmed = outputTopicConfirmed.readRecordsToList()

        assert(recordsRejected.size + recordsConfirmed.size == 1) { "Expected to fail - read test for details" }

        //FIXME test fails because if wallet command related to this order is rejected, it doesn't generate order-command-rejected
        //Proof: following modification passes, and there is no connection in topology between wallet-commands-rejected -> order-commands-rejected
        //val outputTopicWalletCommandRejected = testDriver.createOutputTopic("wallet-commands-rejected", Serdes.String().deserializer(), JsonSerde(WalletCommand::class.java).deserializer())
        //val walletCommandsRejected = outputTopicWalletCommandRejected.readRecordsToList()
        //assert(recordsRejected.size + recordsConfirmed.size + walletCommandsRejected.size == 1)


    }

    @Test
    fun `order submission fails if order with this id already exists`(){
        //TODO implement
        assert(false) { "Not implemented" }
    }

    @Test
    fun `partially filled order is modified in the order-store`(){

        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command-partial-fill.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val order: Order = orderCommand.order!!

        //we have confirmed order
        val orderStore = testDriver.getKeyValueStore<String, Order>("order-store")
        orderStore.put(order.id, order)

        //we have a wallet with blocked amount for that order
        assert(order.assetToBlock() == "BTC")
        assert(order.amountToBlock() == 2.0)
        assert(order.qty == 10.0)

        testDriver.fundWallet(walletId = order.walletId, amount = 2.0, blocked = 2.0, assetIds = arrayOf("BTC") )

        val walletStore = testDriver.getKeyValueStore<String, Wallet>("wallet-store")
        val walletBefore = walletStore.get(order.walletId)
        assert(walletBefore.assets["BTC"]!!.blocked == 2.0)
        assert(walletBefore.assets["BTC"]!!.amount == 2.0)

        val record = TestRecord(orderCommand.orderId, orderCommand)
        input.pipeInput(record)

        val partiallyFilledOrder = orderStore.get(order.id)
        assert(orderCommand.fillQty == 3.5)
        assert(orderCommand.fillPrice == 0.15)

        //gets filled twice as 2 wallet commands processing
        assert(partiallyFilledOrder.qty == 10.0 - 3.5)
        assert(partiallyFilledOrder.qtyFilled == 3.5)

        val walletAfter = walletStore.get(order.walletId)
        assert(walletAfter.assets["BTC"]!!.blocked == 2.0 - (3.5 * 0.2)) //block amount calculated with order price
        assert(walletAfter.assets["BTC"]!!.amount == 2.0 - (3.5 * 0.15)) //actual amount debited with fill price
        assert(walletAfter.assets["ETH"]!!.amount == 3.5)
        assert(walletAfter.assets["ETH"]!!.blocked == 0.0)
    }

    @Test
    fun `filled order gets removed from order-store`(){
       //TODO implement
        assert(false) { "Not implemented" }
    }
}