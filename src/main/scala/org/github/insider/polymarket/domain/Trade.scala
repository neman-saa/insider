package org.github.insider.polymarket.domain

final case class Trade(
  makerAddress: String,
  tokenId: String,
  side: Side,
  amount: BigDecimal,
  totalPrice: BigDecimal,
  txHash: String,
  timestamp: Option[String],
)
