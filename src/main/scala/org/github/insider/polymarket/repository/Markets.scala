package org.github.insider.polymarket.repository

import org.github.insider.polymarket.domain.Market

import java.time.Instant

trait Markets[F[_]] {

  def insert(markets: List[(String, Market)]): F[Int]
  def getMarketClosedTimeByTokenId(tokenId: String): F[Option[Instant]]
}
