package org.github.insider.alchemy.repository

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all._
import doobie.syntax.all._
import doobie.{ConnectionIO, Transactor}
import org.github.insider.polymarket.domain.Trade
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.github.insider.alchemy.repository.codec._

class AggregatedTradesRepositoryImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F])
    extends AggregatedTradesRepository[F] {

  override def insert(trades: NonEmptyList[Trade]): F[Int] =
    createQuery(trades.toList).transact(transactor)

  private def createQuery(trades: List[Trade]): ConnectionIO[Int] = {
    val insert = fr"INSERT INTO agg_trades (maker_address, token_id, data, last_date) VALUES"
    val values = trades
      .map(trade => fr"""
          |(${trade.makerAddress},
          |${trade.tokenId},
          |array(tuple(
          |   ${trade.blockNum},
          |   ${trade.txIndex},
          |   ${trade.side},
          |   ${trade.amount},
          |   ${trade.totalPrice}
          |)),
          |${trade.blockTimestamp}
          |)
          |""".stripMargin)
      .reduce(_ ++ _)
    (insert ++ values).update.run
  }
}

object AggregatedTradesRepositoryImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[AggregatedTradesRepositoryImpl[F]] =
    Slf4jLogger.create[F].map(logger => new AggregatedTradesRepositoryImpl[F](transactor, logger))
}
