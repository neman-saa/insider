package org.github.insider.realtime.traders.scorevector

import org.github.insider.realtime.tokens.TokenId

final case class BuyCandidate(tokenId: TokenId, money: BigDecimal)
