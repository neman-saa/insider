package org.github.insider.alchemy.repository

import org.github.insider.polymarket.domain.Trade

trait TradesRepository[F[_]] {
  def insert(trades: List[Trade]): F[Int]
}
