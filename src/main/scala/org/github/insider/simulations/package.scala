package org.github.insider

import java.time.Instant

package object simulations {
  type TokenId = String

  final case class TokenInfo(
    price: BigDecimal,
    lastPrice: BigDecimal,
    resolveDate: Instant,
    score: BigDecimal
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
  )
}
