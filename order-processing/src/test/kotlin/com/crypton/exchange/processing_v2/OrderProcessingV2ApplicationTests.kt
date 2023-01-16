package com.crypton.exchange.processing_v2

import com.crypton.exchange.events.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.TestRecord
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.JsonSerde

class OrdersExecutionOrderProcessingApplicationTests {

    private lateinit var testDriver: TopologyTestDriver

    private val objectMapper = jacksonObjectMapper()

    @BeforeEach
    fun setup() {
        val topology = singleTopology()
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
    fun `two orders match end-to-end test`() {

        val wallet1id = "wallet_1"
        val wallet2id = "wallet_2"

        val baseAsset = "BTC"
        val quoteAsset = "ETH"

        val initialBalance = 100.0

        //given wallet 1 funded
        testDriver.fundWallet(
            walletId = wallet1id,
            amount = initialBalance,
            blocked = 0.0,
            assetIds = arrayOf(quoteAsset)
        )

        //given wallet 2 funded
        testDriver.fundWallet(
            walletId = wallet2id,
            amount = initialBalance,
            blocked = 0.0,
            assetIds = arrayOf(baseAsset)
        )

        val input = testDriver.createInputTopic(
            PROCESSING_INPUT_TOPIC,
            Serdes.String().serializer(),
            JsonSerde(Event::class.java).serializer()
        )

        val events = listOf(
            //wallet_1 sells 10 ETH, price = 0.05 BTC, expect to have 90 ETH afterwards and 0.5 BTC
            "event-order-submit-match.json",
            //wallet_2 buys 10 ETH, price = 0.05 BTC, expect to have 10 ETH afterwards and 99.5 BTC
            "event-order-submit.json"
        )

        events.map { file ->
            val inputStream = this::class.java.classLoader.getResourceAsStream(file)?.bufferedReader()?.readText()
            objectMapper.readValue(inputStream, Event::class.java)
        }.map { event ->
            TestRecord(event.id, event)
        }.forEach { record ->
            input.pipeInput(record)
        }

        val orderStore = testDriver.getKeyValueStore<String, Order>("order-store")
        val orders = orderStore.all().asSequence().toList()


        //for debug - observing order statuses in output topic
        /*
        val outputTopic = testDriver.createOutputTopic(
            PROCESSING_OUTPUT_TOPIC, Serdes.String().deserializer(), JsonSerde(Event::class.java).deserializer()
        )
        val outputRecords = outputTopic.readValuesToList()
        outputRecords.forEach { println(it.order) }

        println("Orders count in the store: ${orders.size}")
        orders.forEach { println(it) }
        */

        val walletStore = testDriver.getKeyValueStore<String, Wallet>(WALLET_STORE_NAME)

        walletStore.all().asSequence().forEach {
            println(it.value)
        }
        //wallet_1 expected to have 90 ETH afterwards and 0.5 BTC
        val wallet1 = walletStore.get(wallet1id)
        assert(wallet1.assets[baseAsset]?.amount == 0.5)
        assert(wallet1.assets[quoteAsset]?.amount == 90.0)

        //wallet_2 expected to have 10 ETH afterwards and 99.5 BTC
        val wallet2 = walletStore.get(wallet2id)
        assert(wallet2.assets[baseAsset]?.amount == 99.5)
        assert(wallet2.assets[quoteAsset]?.amount == 10.0)


        //assert all existing orders are filled
        orders.forEach {
            assert(it.value.status == OrderStatus.FILLED)
        }

    }

}