package org.github.insider.simulations

import org.github.insider.polymarket.domain.Side

import java.time.Instant

case class SimulationTrade(
  blockTimestamp: Option[Instant],
  blockNum: Int,
  txIndex: Int,
  makerAddress: String,
  tokenId: String,
  side: Side,
  amount: BigDecimal,
  totalPrice: BigDecimal,
  lastPrice: Int,
  closedTime: Instant,
  startTime: Instant,
  marketId: String
)
