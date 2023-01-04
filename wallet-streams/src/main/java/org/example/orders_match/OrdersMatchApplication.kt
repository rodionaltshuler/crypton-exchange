package org.example.orders_match

import org.apache.kafka.streams.KafkaStreams
import org.example.orders_match.processing.ordersMatchTopology
import org.example.orders_execution.properties
import java.util.concurrent.CountDownLatch
import kotlin.system.exitProcess

object OrdersMatchApplication {

    @Throws(Exception::class)
    @JvmStatic
    fun main(args: Array<String>) {

        val props = properties()

        val topology = ordersMatchTopology()

        println("ORDERS MATCH TOPOLOGY: \n ${topology.describe()}")

        val ordersMatchStreams = KafkaStreams(topology, props)

        val latch = CountDownLatch(1)

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(object : Thread("orders-match-stream-shutdown-hook") {
            override fun run() {
                ordersMatchStreams.close()
                latch.countDown()
            }
        })
        try {
            ordersMatchStreams.start()
            latch.await()
        } catch (e: Throwable) {
            exitProcess(1)
        }
        exitProcess(0)
    }


}
