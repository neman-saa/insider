package org.github.insider.polymarket

import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.polymarket.client.EventsClient
import org.github.insider.polymarket.domain.Event
import org.github.insider.polymarket.repository.{Events, EventsImpl, Markets}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.concurrent.duration.DurationInt

class EventsRealtimeFlow[F[_]: Async](
  eventsClient: EventsClient[F],
  logger: Logger[F],
  eventsImpl: Events[F],
  marketsImpl: Markets[F]
) {

  def runForever: F[Unit] = {
    def run(lastClosedTime: Ref[F, Instant]): F[Unit] =
      for {
        lastClosedTimeR <- lastClosedTime.get
        events          <- getAllEventsAfterDate(lastClosedTimeR, 100)
        _               <- eventsImpl.insert(events)
        _ <- marketsImpl.insert(events.flatMap(event => event.markets.getOrElse(Nil).map(market => (event.id, market))))
        _ <- lastClosedTime.set(events.map(_.closedTime.get).maxBy(_.getEpochSecond))
        _ <- Async[F].sleep(3.hour)
      } yield ()

    def getAllEventsAfterDate(date: Instant, limit: Int, offset: Int = 0, events: List[Event] = Nil): F[List[Event]] =
      for {
        newEvents <- eventsClient
          .getLastClosedEvents(limit, offset)
          .map(_.filter(_.closedTime.get.getEpochSecond > date.getEpochSecond))
        res <-
          if (newEvents.length < limit) (newEvents ++ events).pure[F]
          else getAllEventsAfterDate(date, limit, offset + limit, newEvents ++ events)
      } yield res

    for {
      lastTime <- eventsImpl.getLatestClosedDate
      ref      <- Ref.of[F, Instant](lastTime)
      _        <- fs2.Stream.repeatEval(run(ref)).compile.drain
    } yield ()
  }

}

object EventsRealtimeFlow {
  def of[F[_]: Async](
    eventsClient: EventsClient[F],
    eventsImpl: Events[F],
    markets: Markets[F]
  ): F[EventsRealtimeFlow[F]] =
    Slf4jLogger
      .create[F]
      .map(logger => new EventsRealtimeFlow[F](eventsClient, logger, eventsImpl, markets))
}
