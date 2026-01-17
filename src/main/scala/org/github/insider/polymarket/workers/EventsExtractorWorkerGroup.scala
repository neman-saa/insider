package org.github.insider.polymarket.workers

import cats.Parallel
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.polymarket.client.EventsClient
import org.github.insider.polymarket.domain.Event
import org.github.insider.polymarket.repository.{Events, Markets}

import java.time.Instant

private[workers] class EventsExtractorWorkerGroup[F[_]: Async: Parallel](
  eventsClient: EventsClient[F],
  collectedEvents: Ref[F, List[Event]],
  workersNumber: Int,
  marketsImpl: Markets[F],
  eventsImpl: Events[F]
) {

  def getCollectedEvents(maxEndDate: Instant, limit: Int, maxDepth: Int): F[Unit] =
    awaitedRunGroupOfN(maxEndDate, limit, maxDepth) >>
      (for {
        events <- collectedEvents.get
        markets = events.flatMap(event => event.markets.getOrElse(Nil).map(market => (event.id, market)))
        _      <- eventsImpl.insert(events)
        _      <- marketsImpl.insert(markets)
      } yield ())

  private def awaitedRunGroupOfN(maxEndDate: Instant, limit: Int, maxDepth: Int): F[Unit] = {
    for {
      offset <- Ref.of[F, Int](0)
      workers <-
        (1 to workersNumber)
          .map(n => EventsExtractorWorker.of[F](eventsClient, offset, collectedEvents, n))
          .toList
          .sequence
      _ <- workers.parTraverse_(_.run(maxEndDate, limit, maxDepth))
    } yield ()
  }
}

object EventsExtractorWorkerGroup {
  def of[F[_]: Async: Parallel](
    eventsClient: EventsClient[F],
    marketsImpl: Markets[F],
    eventsImpl: Events[F]
  )(workersNumber: Int = 1): F[EventsExtractorWorkerGroup[F]] =
    for {
      collectedEvents <- Ref.of[F, List[Event]](List.empty)
    } yield new EventsExtractorWorkerGroup[F](eventsClient, collectedEvents, workersNumber, marketsImpl, eventsImpl)
}
