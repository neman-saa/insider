package org.github.insider.alchemy.repository

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all._
import doobie.{Transactor, Update}
import org.github.insider.polymarket.domain.Trade
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import doobie.syntax.all._
import doobie.postgres.implicits._

class TradesRepositoryImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F]) extends TradesRepository[F] {

  override def insert(trades: NonEmptyList[Trade]): F[Int] =
    Update[Trade](
      """
      |INSERT INTO trades (maker_address, token_id, side, amount, total_price, block_num, tx_hash, tx_index, block_timestamp)
      |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      |""".stripMargin
    )
      .updateMany(trades)
      .transact(transactor)
      .flatTap(rows => logger.info(s"Inserted $rows trades in 'trades' table"))

  override def getLatestBlock: F[Long] =
    fr"""
      |SELECT ifNull(max(block_num), 51500000) FROM trades
      |"""
      .stripMargin
      .query[Long]
      .unique
      .transact(transactor)

  override def getHistoricalTrades(limit: Int, offset: Int): F[List[Trade]] =
    sql"select * from trades order by (block_num, tx_index) limit $limit offset $offset"
      .query[Trade]
      .to[List]
      .transact(transactor)
}

object TradesRepositoryImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[TradesRepositoryImpl[F]] =
    Slf4jLogger.create[F].map(logger => new TradesRepositoryImpl[F](transactor, logger))
}
