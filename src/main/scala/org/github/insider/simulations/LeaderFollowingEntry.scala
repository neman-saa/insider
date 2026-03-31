package org.github.insider.simulations

import cats.data.NonEmptyList
import org.github.insider.leaderboard.HexAddress
import org.github.insider.polymarket.domain.Side
import org.github.insider.simulations.LeaderFollowingEntry.TokenOperation

final case class LeaderFollowingEntry(
  leader: HexAddress,
  leaderFirstBuy: BigDecimal,
  ourTotalPrice: BigDecimal,
  ourTotalPricePutIn: BigDecimal,
  allowedTotalPrice: BigDecimal,
  ourAmount: BigDecimal
)

object LeaderFollowingEntry {
  final case class TokenOperation(
    side: Side,
    amount: BigDecimal,
    singleTokenPrice: BigDecimal,
    dealtAtBlock: Int,
  )
}
