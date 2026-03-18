package org.github.insider.polymarket.workers

import cats.data.NonEmptyList
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.polymarket.client.EventsClient
import org.github.insider.polymarket.domain.Event
import org.github.insider.polymarket.repository.{Events, Markets}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

private[workers] class EventsExtractorWorker[F[_]: Async](
  offset: Ref[F, Int],
  eventsClient: EventsClient[F],
  marketsImpl: Markets[F],
  eventsImpl: Events[F],
  limit: Int,
  workerNumber: Int,
)(logger: Logger[F]) {

  def run: F[Unit] = {
    offset.getAndUpdate(_ + limit).flatMap { currentOffset =>
      runFor(currentOffset, limit).flatMap { continue =>
        if (continue) run
        else logger.info(s"[Worker - $workerNumber] Finished")
      }
    }
  }

  private def runFor(offset: Int, limit: Int): F[Boolean] =
    for {
      _      <- logger.info(s"[Worker - $workerNumber] Running range: $offset - ${offset + limit}")
      events <- eventsClient.getClosedEventsSortedByClosedTime(limit, offset)
      _      <- logger.info(s"[Worker - $workerNumber] Events fetched: ${events.size}")

      markets = events.flatMap(event => event.markets.getOrElse(Nil).map(market => (event.id, market)))

      maybeEventsNel  = NonEmptyList.fromList(events)
      maybeMarketsNel = NonEmptyList.fromList(markets)

      _ <- maybeEventsNel.fold(0.pure[F])(eventsImpl.insert)
      _ <- maybeMarketsNel.fold(0.pure[F])(marketsImpl.insert)

      _ <- logger.info(s"[Worker - $workerNumber] Finished range: $offset - ${offset + limit}")
    } yield events.nonEmpty
}

private[workers] object EventsExtractorWorker {
  def of[F[_]: Async](
    offset: Ref[F, Int],
    eventsClient: EventsClient[F],
    marketsImpl: Markets[F],
    eventsImpl: Events[F],
    limit: Int,
    workerN: Int,
  ): F[EventsExtractorWorker[F]] =
    Slf4jLogger
      .create[F]
      .map(logger =>
        new EventsExtractorWorker[F](offset, eventsClient, marketsImpl, eventsImpl, limit, workerN)(logger)
      )
}
