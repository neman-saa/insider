package org.github.insider.alchemy.repository

import cats.data.NonEmptyList
import org.github.insider.polymarket.domain.Trade

trait TradesRepository[F[_]] {
  def insert(trades: NonEmptyList[Trade]): F[Int]
  def getLatestBlock: F[Long]
}
