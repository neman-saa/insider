package org.github.insider.alchemy.repository

import cats.effect.Async
import doobie.{Transactor, Update}
import org.github.insider.polymarket.domain.Trade
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._
import doobie.syntax.all._
import org.github.insider.alchemy.repository.codec._

class TradesRepositoryImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F]) extends TradesRepository[F] {

  override def insert(trades: List[Trade]): F[Int] =
    Update[Trade](
      """
        |INSERT INTO trades_v2 (maker_address, token_id, side, amount, total_price, block_num, tx_hash, tx_index, block_timestamp)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin
    )
      .updateMany(trades)
      .transact(transactor)

  override def getLatestBlock: F[Long] =
    fr"""
      |SELECT ifNull(max(block_num), 51500000) FROM trades_v2
      |"""
      .stripMargin
      .query[Long]
      .unique
      .transact(transactor)
}

object TradesRepositoryImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[TradesRepositoryImpl[F]] =
    Slf4jLogger.create[F].map(logger => new TradesRepositoryImpl[F](transactor, logger))
}
