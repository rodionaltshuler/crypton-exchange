package org.example.domain

interface HasOrderId {
    fun orderId(): String
}
enum class OrderType {
    LIMIT_SELL, LIMIT_BUY
}

//TODO Order can be NEW or CONFIRMED, as orders with other statuses are not supposed to be written in stateStore/streams
//When Order rejected, filled or cancelled, it should be just removed from order store
//REJECTED status should be applied to OrderCommand instead
enum class OrderStatus {
    NEW, CONFIRMED, PARTIALLY_FILLED, FILLED, CANCELLED
}

data class Order(
    val id: String,
    val baseAssetId: String,
    val quoteAssetId: String,
    val walletId: String,
    val orderType: OrderType,
    val price: Double = 1.0, //price == quote asset price / base asset price
    val qty: Double = 0.0, //quote asset qty
    val qtyFilled: Double = 0.0,
    val status: OrderStatus = OrderStatus.NEW
): HasOrderId {

    // BTC-USD market, BTC is quote asset, USD is base
    // BUY order, qty = 2.0, price = 15 000: I want to buy 2 BTC
    // Amount to block: I need qty * price = 30 000 USD (base asset)
    // Asset to block: USD (base)
    fun assetToBlock() =
        when (orderType) {
            OrderType.LIMIT_BUY -> baseAssetId //buying quote asset, so need base asset in exchange
            OrderType.LIMIT_SELL -> quoteAssetId
        }


    // BTC-USD market, BTC is quote asset, USD is base
    // SELL order, qty = 2.0, price = 15 000: I want to sell 2 BTC
    // Amount to block: 2.0
    // Asset to block: BTC (quote)
    fun amountToBlock() = when (orderType) {
        OrderType.LIMIT_BUY -> qty * price //buying quote asset, so need baseAsset
        OrderType.LIMIT_SELL -> qty //selling quote asset, and QTY is for quote asset
    }

    fun amountReleaseOnFill(fillQty: Double): Double =
        when (orderType) {
            OrderType.LIMIT_BUY -> fillQty * price
            OrderType.LIMIT_SELL -> fillQty //selling quote asset, and QTY is for quote asset
        }


    fun assetToDebit() = assetToBlock()

    fun amountToDebit(fillPrice: Double, fillQty: Double) = when (orderType) {
        OrderType.LIMIT_BUY -> fillQty * fillPrice //buying quote asset, so need baseAsset
        OrderType.LIMIT_SELL -> fillQty //selling quote asset, and QTY is for quote asset
    }

    //opposite to block asset
    fun assetToCredit() = when (orderType) {
        OrderType.LIMIT_BUY -> quoteAssetId
        OrderType.LIMIT_SELL -> baseAssetId
    }

    fun amountToCredit(fillPrice: Double, fillQty: Double) = when (orderType) {
        OrderType.LIMIT_BUY -> fillQty //buying quote asset, so it will be credited, and qty is of quoteAsset
        OrderType.LIMIT_SELL -> fillPrice * fillQty
    }

    override fun orderId() = id


}