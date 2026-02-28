package org.github.insider.polymarket.domain

import java.time.LocalDateTime

final case class Trade(
  makerAddress: String,
  tokenId: String,
  side: Side,
  amount: BigDecimal,
  totalPrice: BigDecimal,
  blockNum: Long,
  txHash: String,
  txIndex: Int,
  blockTimestamp: Option[LocalDateTime],
)
