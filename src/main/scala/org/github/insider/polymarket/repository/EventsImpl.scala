package org.github.insider.polymarket.repository

import cats.effect.kernel.Async
import cats.syntax.all._
import doobie.implicits.javatimedrivernative._
import doobie.syntax.all._
import doobie.{Transactor, Update}
import org.github.insider.polymarket.domain.{Event, Volume}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.{Instant, OffsetDateTime, ZoneOffset, ZonedDateTime}

class EventsImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F]) extends Events[F] {

  override def insert(events: List[Event]): F[Int] =
    Update[(String, Option[Volume], String, OffsetDateTime, Option[OffsetDateTime], String)](
      """
      |INSERT INTO events (id, volume, title, created_at, closed_time, tags)
      |VALUES (?, ?, ?, ?, ?, splitByChar(',', ?))
      |""".stripMargin
    ).updateMany(
      events.map(event =>
        (
          event.id,
          event.volume,
          event.title,
          event.createdAt.atOffset(ZoneOffset.UTC),
          event.closedTime.map(_.atOffset(ZoneOffset.UTC)),
          event.tags.getOrElse(Nil).flatMap(_.label).mkString(",")
        )
      )
    ).transact(transactor)

  override def getLatestClosedDate: F[Instant] =
    fr"""
        |SELECT ifNull(max(closed_time), toDateTime64(1609459200, 6)) FROM events
        |"""
      .stripMargin
      .query[ZonedDateTime]
      .map(Instant.from(_))
      .unique
      .transact(transactor)
}

object EventsImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[Events[F]] = {
    val loggerF = Slf4jLogger.create[F]
    loggerF.map(logger => new EventsImpl[F](transactor, logger))
  }
}
