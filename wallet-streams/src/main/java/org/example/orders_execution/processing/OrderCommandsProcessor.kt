package org.example.orders_execution.processing

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.example.domain.Order
import org.example.domain.OrderCommand
import org.example.domain.OrderCommandType
import org.example.domain.OrderStatus
import org.springframework.kafka.support.serializer.JsonSerde


val orderStoreBuilder: StoreBuilder<*> = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("order-store"),
    Serdes.String(),
    JsonSerde(Order::class.java)
)


val orderCommandsStoreBuilder: StoreBuilder<*> = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("order-commands-store"),
    Serdes.String(),
    JsonSerde(OrderCommand::class.java)
)

class OrderCommandsProcessor : Processor<String, OrderCommand, String, OrderCommand> {

    private lateinit var orderStore: ReadOnlyKeyValueStore<String, Order>

    private lateinit var orderCommandsStore: KeyValueStore<String, OrderCommand>

    private lateinit var context: ProcessorContext<String, OrderCommand>

    override fun init(context: ProcessorContext<String, OrderCommand>?) {
        super.init(context)
        orderStore = context!!.getStateStore("order-store")
        orderCommandsStore = context.getStateStore("order-commands-store")
        this.context = context
    }

    override fun process(record: Record<String, OrderCommand>?) {
        val command = record!!.value()
        orderCommandsStore.put(command.id, command)
        when (command.command) {
            OrderCommandType.SUBMIT -> {
                val existingOrder = orderStore.get(command.orderId)
                if (existingOrder == null) {
                    val orderCommand = Record(
                        command.orderId,
                        command,
                        context.currentSystemTimeMs()
                    )
                    context.forward(orderCommand, "OrderCommandToWalletCommandsProcessor")
                } else {
                    val rejectedOrderCommand = Record(
                        command.orderId,
                        command.copy(message = "SUBMIT command for the order which already exists"),
                        context.currentSystemTimeMs()
                    )
                    context.forward(rejectedOrderCommand, "RejectedOrderCommandsProcessor")
                }
            }

            OrderCommandType.CANCEL -> {

                val existingOrder = orderStore.get(command.orderId)
                if (existingOrder == null || existingOrder.status != OrderStatus.CONFIRMED) {
                    val rejectedOrderCommand = Record(
                        command.orderId,
                        command.copy(message = "Can only CANCEL orders with status CONFIRMED"),
                        context.currentSystemTimeMs()
                    )
                    context.forward(rejectedOrderCommand, "RejectedOrderCommandsProcessor")
                } else {
                    val orderCommand = Record(
                        command.orderId,
                        command,
                        context.currentSystemTimeMs()
                    )
                    context.forward(orderCommand, "OrderCommandToWalletCommandsProcessor")
                }

            } //release funds
            OrderCommandType.FILL -> {
                val existingOrder = orderStore.get(command.orderId)

                if (existingOrder != null &&
                    setOf(OrderStatus.CONFIRMED, OrderStatus.PARTIALLY_FILLED).contains(existingOrder.status)
                ) {

                    if (command.fillQty <= existingOrder.qty) {
                        val orderCommand = Record(
                            command.orderId,
                            command,
                            context.currentSystemTimeMs()
                        )
                        context.forward(orderCommand, "OrderCommandToWalletCommandsProcessor")
                    } else {
                        val rejectedOrderCommand = Record(
                            command.orderId,
                            command.copy(message = "Fill qty ${command.fillQty} is more than order qty ${existingOrder.qty} "),
                            context.currentSystemTimeMs()
                        )
                        context.forward(rejectedOrderCommand, "RejectedOrderCommandsProcessor")
                    }

                } else {
                    val rejectedOrderCommand = Record(
                        command.orderId,
                        command.copy(message = "Can fill only CONFIRMED orders"),
                        context.currentSystemTimeMs()
                    )
                    context.forward(rejectedOrderCommand, "RejectedOrderCommandsProcessor")
                }
            } //unblock and debit one asset and credit another
        }

    }

}
