package org.github.insider.polymarket.workers

import cats.Parallel
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.polymarket.client.EventsClient
import org.github.insider.polymarket.domain.Event
import org.github.insider.polymarket.repository.{Events, Markets}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

private[workers] class EventsExtractorWorkerGroup[F[_]: Async: Parallel](
  eventsClient: EventsClient[F],
  workersNumber: Int,
  marketsImpl: Markets[F],
  eventsImpl: Events[F],
  limit: Int,
)(logger: Logger[F]) {
  def extractAllClosedEvents: F[Unit] = {
    for {
      _      <- logger.info(s"Starting events extraction")
      offset <- Ref.of[F, Int](0)
      workers <-
        (1 to workersNumber)
          .map(n => EventsExtractorWorker.of[F](offset, eventsClient, marketsImpl, eventsImpl, limit, n))
          .toList
          .sequence
      _ <- workers.parTraverse_(_.run)
    } yield ()
  }
}

object EventsExtractorWorkerGroup {
  def of[F[_]: Async: Parallel](
    eventsClient: EventsClient[F],
    marketsImpl: Markets[F],
    eventsImpl: Events[F],
    limit: Int,
  )(workersNumber: Int): F[EventsExtractorWorkerGroup[F]] =
    Slf4jLogger
      .create[F]
      .map(logger =>
        new EventsExtractorWorkerGroup[F](eventsClient, workersNumber, marketsImpl, eventsImpl, limit)(logger)
      )
}
