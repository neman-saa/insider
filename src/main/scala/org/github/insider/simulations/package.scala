package org.github.insider

import java.time.Instant

package object simulations {
  type TokenId = String

  final case class TokenResolutionInfo(
    lastPrice: BigDecimal,
    resolveDate: Instant
  )

  final case class SimulationConfig(
    // simulation flow
    blocksProcessingBatchSize: Int,
    // leaderboard
    leaderboardLimit: Int,
    leaderboardBlocksLifetime: Int,
    // wallets
    initialWalletBalance: BigDecimal,
    minWalletBlocksLifetime: Int,
    maxWalletBlocksLifetime: Int,
    maxTemporaryWalletsInPool: Int,
    // buy sell processing
    extraBuyPerCents: BigDecimal,
    allowedPerCentsPerUser: BigDecimal
  )
}
