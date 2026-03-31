package org.github.insider.alchemy.repository

import cats.data.NonEmptyList
import org.github.insider.polymarket.domain.Trade

import java.time.Instant

trait TradesRepository[F[_]] {
  def insert(trades: NonEmptyList[Trade]): F[Int]
  def getLatestBlock: F[Long]
  def getHistoricalTrades(limit: Long, offset: Int): F[List[Trade]]
  def getEarliestTradeTimestamp: F[Instant]
}
