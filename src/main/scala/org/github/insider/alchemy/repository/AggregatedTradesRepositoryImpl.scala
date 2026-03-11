package org.github.insider.alchemy.repository

import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.all._
import doobie.syntax.all._
import doobie.{Fragment, Transactor}
import org.github.insider.polymarket.domain.Trade
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.github.insider.alchemy.repository.codec._

import java.time.ZoneOffset

class AggregatedTradesRepositoryImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F])
    extends AggregatedTradesRepository[F] {

  override def insert(trades: NonEmptyList[Trade]): F[Int] =
    createQuery(trades.toList)
      .update
      .run
      .transact(transactor)
      .flatTap(rows => logger.info(s"Inserted $rows trades in 'agg_trades' table"))

  private def createQuery(trades: List[Trade]): Fragment = {
    val insert = fr"INSERT INTO agg_trades (maker_address, token_id, data, last_activity_timestamp) VALUES"
    val values = trades
      .map(trade => fr"""
          |(
          |   ${trade.makerAddress},
          |   ${trade.tokenId},
          |   array(
          |       tuple(
          |           ${trade.blockNum},
          |           ${trade.txIndex},
          |           ${trade.side},
          |           ${trade.amount},
          |           ${trade.totalPrice}
          |       )
          |   ),
          |   ${trade.blockTimestamp.map(_.toInstant(ZoneOffset.UTC).toEpochMilli)}
          |)
          |""".stripMargin)
      .reduce(_ ++ _)

    insert ++ values
  }
}

object AggregatedTradesRepositoryImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[AggregatedTradesRepositoryImpl[F]] =
    Slf4jLogger.create[F].map(logger => new AggregatedTradesRepositoryImpl[F](transactor, logger))
}
