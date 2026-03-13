package org.github.insider.polymarket.repository

import cats.effect.kernel.Async
import cats.syntax.all._
import doobie.{Transactor, Update}
import doobie.implicits._
import doobie.postgres.implicits._
import org.github.insider.polymarket.domain.{Market, Volume}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import java.time.{OffsetDateTime, ZoneOffset}

class MarketsImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F]) extends Markets[F] {

  override def insert(markets: List[(String, Market)]): F[Int] = {
    val marketsConnection =
      Update[
        (
          String,
          String,
          String,
          OffsetDateTime,
          Option[OffsetDateTime],
          String,
          Option[Volume],
          Option[OffsetDateTime],
          Option[OffsetDateTime]
        )
      ](
        """
        |INSERT INTO markets(
        | id,
        | condition_id,
        | question,
        | created_at,
        | closed_time,
        | event_id,
        | volume,
        | startDate,
        | endDate
        |)
        |VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        |""".stripMargin
      ).updateMany(markets.map { t =>
        val tuple =
          (
            t._2.id,
            t._2.conditionId,
            t._2.question,
            t._2.createdAt.atOffset(ZoneOffset.UTC),
            t._2.closedTime.map(_.atOffset(ZoneOffset.UTC)),
            t._1,
            t._2.volume,
            t._2.startDate.map(_.atOffset(ZoneOffset.UTC)),
            t._2.endDate.map(_.atOffset(ZoneOffset.UTC))
          )
        tuple
      })

    val outcomeTokensConnection =
      Update[(Option[String], String, Option[String], Option[Volume])]("""
         |INSERT INTO tokens (id, market_id, outcome, last_price)
         |VALUES (?, ?, ?, ?)
         |""".stripMargin).updateMany(
        markets
          .map(market => (market._2.id, market._2.tokens))
          .flatMap(t => t._2.map(token => (token.id, t._1, token.outcome, token.lastPrice)))
      )

    (for {
      n <- marketsConnection
      _ <- outcomeTokensConnection
    } yield n).transact(transactor)
  }
}

object MarketsImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[Markets[F]] = {
    val loggerF = Slf4jLogger.create[F]
    loggerF.map(logger => new MarketsImpl[F](transactor, logger))
  }
}
