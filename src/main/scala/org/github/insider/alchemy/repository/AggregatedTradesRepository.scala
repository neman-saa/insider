package org.github.insider.alchemy.repository

import cats.data.NonEmptyList
import org.github.insider.polymarket.domain.Trade

trait AggregatedTradesRepository[F[_]] {
  def insert(trades: NonEmptyList[Trade]): F[Int]
}
