package org.github.insider.polymarket.repository

import cats.effect.kernel.Async
import cats.syntax.all._
import doobie.implicits._
import doobie.implicits.javatimedrivernative._
import doobie.{Transactor, Update}
import org.github.insider.polymarket.domain.{Event, Volume}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.{OffsetDateTime, ZoneOffset}

class EventsImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F]) extends Events[F] {

  override def insert(events: List[Event]): F[Int] =
    Update[(String, Option[Volume], String, Option[OffsetDateTime], Option[OffsetDateTime], String)](
      """
      |INSERT INTO events (id, volume, title, start_date, end_date, tags)
      |VALUES (?, ?, ?, ?, ?, string_to_array(?, ','))
      |""".stripMargin
    ).updateMany(
      events.map(event =>
        (
          event.id,
          event.volume,
          event.title,
          event.startDate.map(_.atOffset(ZoneOffset.UTC)),
          event.endDate.map(_.atOffset(ZoneOffset.UTC)),
          event.tags.getOrElse(Nil).flatMap(_.label).mkString(",")
        )
      )
    ).transact(transactor)
}

object EventsImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[Events[F]] = {
    val loggerF = Slf4jLogger.create[F]
    loggerF.map(logger => new EventsImpl[F](transactor, logger))
  }
}
