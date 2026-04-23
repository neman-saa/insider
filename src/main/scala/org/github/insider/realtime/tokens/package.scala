package org.github.insider.realtime

import java.time.Instant

package object tokens {
  type TokenId = String

  final case class TokenInfo(
    id: TokenId,
    price: BigDecimal,
    score: BigDecimal,
    resolveDate: Instant,
    lastUpdatedBlock: Long,
  )

  final case class TokenMetaInfo(
    tokenId: TokenId,
    oppositeTokenId: TokenId,
    resolveDate: Instant,
  )
}
