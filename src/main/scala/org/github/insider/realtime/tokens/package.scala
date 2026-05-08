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
    buyPrice: Option[BigDecimal],
    buyTime: Option[Instant]
  )

  final case class TokenInfoShort(
    id: String,
    efficiency: BigDecimal,
    buyTime: Option[Instant],
    price: BigDecimal,
    resolveDate: Instant,
    score: BigDecimal
  )

  final case class TokenMetaInfo(
    tokenId: TokenId,
    oppositeTokenId: TokenId,
    resolveDate: Instant,
  )
}
