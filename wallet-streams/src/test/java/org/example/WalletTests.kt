package org.example

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.TestRecord
import org.example.domain.*
import org.example.orders_execution.ordersExecutionTopology
import org.example.orders_execution.properties
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.JsonSerde

class WalletTests {

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
    fun `Order LIMIT_BUY submission successful when there are enough funds`(){
        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command-submit-order.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)

        val record = TestRecord(orderCommand.orderId, orderCommand)

        val order = orderCommand.order!! //BTC-ETH: buying 10 ETH price 0.05 -> expect to block 0.5 BTC
        val btcInitialAmount = 10.0
        testDriver.fundWallet(order.walletId, btcInitialAmount, 0.0, order.baseAssetId)

        input.pipeInput(record)

        val orderStore = testDriver.getKeyValueStore<String, Order>("order-store")
        assert(orderStore[order.id].status == OrderStatus.CONFIRMED)

    }

    @Test
    fun `Order LIMIT_BUY submission rejected when there are funds insufficient`(){

        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command-submit-order.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)

        val record = TestRecord(orderCommand.orderId, orderCommand)

        val order = orderCommand.order!! //BTC-ETH: buying 10 ETH price 0.05 -> expect to block 0.5 BTC
        //making sure balance is 0
        testDriver.fundWallet(order.walletId, 0.0, 0.0, order.baseAssetId)

        input.pipeInput(record)

        val rejectedWalletCommands = testDriver.createOutputTopic("wallet-commands-rejected", Serdes.String().deserializer(), JsonSerde(
            WalletCommand::class.java).deserializer())

        val actualOutputRecord = rejectedWalletCommands.readRecord()

        //Wallet rejected command is generated
        assert(actualOutputRecord != null)
        assert(actualOutputRecord.value.walletId == order.walletId)

        //And order is not actually present in order store
        val orderStore = testDriver.getKeyValueStore<String, Order>("order-store")
        assert(orderStore[order.id] == null)
    }

    @Test
    fun `When LIMIT_BUY order submitted proper assets amount is blocked in wallet`(){
        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command-submit-order.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)

        val record = TestRecord(orderCommand.orderId, orderCommand)

        val order = orderCommand.order!! //BTC-ETH: buying 10 ETH price 0.05 -> expect to block 0.5 BTC
        val btcInitialAmount = 10.0
        val btcBlocked = 0.5
        testDriver.fundWallet(order.walletId, btcInitialAmount, 0.0, order.baseAssetId)

        input.pipeInput(record)

        val walletStore = testDriver.getKeyValueStore<String, Wallet>("wallet-store")
        val baseAssetInWallet = walletStore.get(order.walletId).assets[order.baseAssetId]

        assert(baseAssetInWallet!!.blocked == btcBlocked)

    }

    @Test
    fun `When LIMIT_BUY order cancelled proper assets amount is released in wallet`(){
        val input = testDriver.createInputTopic("order-commands", Serdes.String().serializer(), JsonSerde(OrderCommand::class.java).serializer())
        val inputStream = this::class.java.classLoader.getResourceAsStream("order-command-cancel.json")?.bufferedReader()?.readText()
        val orderCommand = objectMapper.readValue(inputStream, OrderCommand::class.java)
        val record = TestRecord(orderCommand.orderId, orderCommand.copy(order = null))
        val order = orderCommand.order!!

        //putting the order in store so we can cancel it
        val orderStore = testDriver.getKeyValueStore<String, Order>("order-store")
        orderStore.put(order.id, order.copy(status = OrderStatus.CONFIRMED))

        val btcInitialAmount = 10.0
        val btcInitiallyBlocked = 1.0 //BTC-ETH: buying 10 ETH price 0.05 -> expect to release 0.5 BTC
        val btcBlockedAfterRelease = 1.0 - 0.5
        testDriver.fundWallet(order.walletId, btcInitialAmount, btcInitiallyBlocked, order.baseAssetId)

        input.pipeInput(record)

        val walletStore = testDriver.getKeyValueStore<String, Wallet>("wallet-store")
        val baseAssetInWallet = walletStore.get(order.walletId).assets[order.baseAssetId]

        assert(baseAssetInWallet!!.blocked == btcBlockedAfterRelease)
    }

    @Test
    fun `LIMIT_BUY fill - proper asset released, debited and credited`(){
        //TODO implement
        assert(false) { "Not implemented" }
    }

    @Test
    fun `LIMIT_BUY fill - order removed from order-store`(){
        //TODO implement
        assert(false) { "Not implemented" }
    }

    @Test
    fun `LIMIT_BUY partial fill - proper asset released, debited and credited`(){
        //TODO implement
        assert(false) { "Not implemented" }
    }

    @Test
    fun `LIMIT_BUY partial fill - order modified in order-store`(){
        //TODO implement
        assert(false) { "Not implemented" }
    }

}

