package org.github.insider.polymarket.repository

import cats.effect.kernel.Async
import cats.syntax.all._
import doobie.implicits.javatimedrivernative._
import doobie.{Fragment, Transactor}
import org.github.insider.polymarket.domain.Event
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.ZoneOffset

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
    createQuery(events).update.run.transact(transactor)

  private def createQuery(events: List[Event]): Fragment = {
    val insert = fr"INSERT INTO events (id, volume, title, start_date, end_date, tags) VALUES"
    val values = events
      .map(event => fr"""
             |(
             |    ${event.id},
             |    ${event.volume},
             |    ${event.title},
             |    ${event.startDate.map(_.atOffset(ZoneOffset.UTC))},
             |    ${event.endDate.map(_.atOffset(ZoneOffset.UTC))},
             |    splitByChar(',', ${event.tags.getOrElse(Nil).flatMap(_.label).mkString(",")})
             |)
             |""".stripMargin)
      .reduce(_ ++ _)

    insert ++ values
  }
}

object EventsImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[Events[F]] = {
    val loggerF = Slf4jLogger.create[F]
    loggerF.map(logger => new EventsImpl[F](transactor, logger))
  }
}
