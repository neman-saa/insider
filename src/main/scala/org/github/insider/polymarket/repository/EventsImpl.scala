package org.github.insider.polymarket.repository

import cats.effect.kernel.Async
import cats.syntax.all._
import doobie.implicits.javatimedrivernative._
import doobie.syntax.all._
import doobie.{ConnectionIO, Transactor}
import org.github.insider.polymarket.domain.Event
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.{Instant, ZonedDateTime}

class EventsImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F]) extends Events[F] {

  private def createQuery(events: List[Event]): ConnectionIO[Int] = {
    val insert = fr"INSERT INTO events (id, volume, title, created_at, closed_time, tags) VALUES"
    val values = events
      .map(event => fr"""
                        |(
                        |${event.id},
                        |${event.volume},
                        |${event.title},
                        |${event.createdAt},
                        |${event.closedTime},
                        |splitByChar(',', ${event.tags.getOrElse(Nil).flatMap(_.label).mkString(",")})
                        |)
                        |""".stripMargin)
      .reduce(_ ++ _)
    (insert ++ values).update.run
  }

  override def insert(events: List[Event]): F[Int] =
    createQuery(events)
      .transact(transactor)
      .flatTap(n =>
        logger.info(s"Inserted $n events") >> logger.info(s"Inserted ${events.flatMap(_.markets).length} markets")
      )

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
