package org.github.insider.polymarket.repository

import cats.effect.kernel.Async
import cats.syntax.all._
import doobie.{Transactor, Update}
import doobie.implicits._
import doobie.postgres.implicits._
import org.github.insider.polymarket.domain.{Market, Side, Token, Volume}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.{Instant, OffsetDateTime, ZoneOffset}

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

  override def getMarketByTokenId(tokenId: String): F[Option[Market]] =
    sql"""
       SELECT
          m.id,
          any(m.question) AS question,
          any(m.condition_id) AS condition_id,
          any(m.volume) AS volume,
          groupArray(tuple(t.outcome, t.id, t.last_price) AS token_ids,
          any(m.created_at) AS created_at,
          any(m.closed_time) AS closed_time,
          any(m.startDate) AS start_date,
          any(m.endDate) AS end_date
       FROM markets m
       JOIN tokens t ON t.market_id = m.id
       WHERE m.id IN (
           SELECT market_id
           FROM tokens
           WHERE id = $tokenId
       )
       GROUP BY m.id
     """
      .query[
        (
          String,
          String,
          String,
          Option[BigDecimal],
          List[(Option[String], Option[String], Option[BigDecimal])],
          Instant,
          Option[Instant],
          Option[Instant],
          Option[Instant]
        )
      ]
      .option
      .transact(transactor)
      .map(
        _.map {
          case (id, question, conditionId, volume, tokenTuples, createdAt, closedTime, startDate, endDate) =>
            Market(
              id,
              question,
              conditionId,
              volume.map(Volume.apply),
              tokenTuples.map { case (tid, tside, tlprice) => Token(tid, tside, tlprice.map(Volume.apply)) },
              createdAt,
              closedTime,
              None,
              startDate,
              endDate
            )
        }
      )
}

object MarketsImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[Markets[F]] = {
    val loggerF = Slf4jLogger.create[F]
    loggerF.map(logger => new MarketsImpl[F](transactor, logger))
  }
}
