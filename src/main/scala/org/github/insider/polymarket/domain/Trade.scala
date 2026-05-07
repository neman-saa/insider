package org.github.insider.polymarket.domain

import org.github.insider.polymarket.CTFType

import java.time.LocalDateTime
import scala.math.BigDecimal.RoundingMode

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
  ctfType: CTFType,
) {
  val singleTokenPrice: BigDecimal = (totalPrice / (amount / BigDecimal(1_000_000))).setScale(2, RoundingMode.HALF_UP)
}
