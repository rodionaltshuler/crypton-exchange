package org.example.order

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.example.Order
import org.example.OrderCommand
import org.example.OrderCommandType
import org.example.OrderStatus
import org.springframework.kafka.support.serializer.JsonSerde


val orderStoreBuilder: StoreBuilder<*> = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("order-store"),
    Serdes.String(),
    JsonSerde(Order::class.java)
)


class OrderCommandsProcessor : Processor<String, OrderCommand, String, OrderCommand> {

    private lateinit var orderStore: ReadOnlyKeyValueStore<String, Order>

    private lateinit var context: ProcessorContext<String, OrderCommand>

    override fun init(context: ProcessorContext<String, OrderCommand>?) {
        super.init(context)
        orderStore = context!!.getStateStore("order-store")
        this.context = context
    }

    override fun process(record: Record<String, OrderCommand>?) {
        val command = record!!.value()
        when (command.command) {
            OrderCommandType.SUBMIT -> {
                if (command.order.status == OrderStatus.NEW) {
                    val orderCommand = Record(
                        command.orderId,
                        command,
                        context.currentSystemTimeMs()
                    )
                    context.forward(orderCommand, "OrderCommandToWalletCommandsProcessor")

                } else {
                    val rejectedOrderCommand = Record(
                        command.orderId,
                        command.copy(message = "SUBMIT command is applicable only to NEW orders"),
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
                //FIXME check state store if this order exists and confirmed

                val existingOrder = orderStore.get(command.orderId)
                if (existingOrder != null && existingOrder.status == OrderStatus.CONFIRMED) {
                    val orderCommand = Record(
                        command.orderId,
                        command,
                        context.currentSystemTimeMs()
                    )
                    context.forward(orderCommand, "OrderCommandToWalletCommandsProcessor")

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
