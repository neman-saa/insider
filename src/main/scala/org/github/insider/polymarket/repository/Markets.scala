package org.github.insider.polymarket.repository

import org.github.insider.polymarket.domain.Market

trait Markets[F[_]] {

  def insert(markets: List[(String, Market)]): F[Int]
  def getMarketByTokenId(tokenId: String): F[Option[Market]]
}
