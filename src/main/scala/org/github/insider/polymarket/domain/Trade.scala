package org.github.insider.polymarket.domain

case class Trade(
  makerAddress: String,
  assetId: String,
  size: BigDecimal,
  totalPrice: BigDecimal,
  timestamp: Option[String],
  hash: String,
  side: Side
)
