package org.github.insider.polymarket.workers

import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.polymarket.client.EventsClient
import org.github.insider.polymarket.domain.Event
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

private[workers] class EventsExtractorWorker[F[_]: Async](
  eventsClient: EventsClient[F],
  offset: Ref[F, Int],
  collectedEvents: Ref[F, List[Event]],
  logger: Logger[F],
)(workerNumber: Int) {

  def run(maxEndDate: Instant, limit: Int, maxDepth: Int): F[Unit] =
    offset.getAndUpdate(_ + limit).flatMap {
      case currentOffset if maxDepth < currentOffset => Async[F].unit
      case currentOffset =>
        val action =
          for {
            events <- eventsClient.getEventsByMaxEndDate(maxEndDate, limit, currentOffset)
            _ <- logger
              .info(
                s"[Worker - $workerNumber] Events received: total: $currentOffset"
              )
            _ <- collectedEvents.update(_.appendedAll(events))
          } yield ()

        action >> run(maxEndDate, limit, maxDepth)
    }
}

private[workers] object EventsExtractorWorker {
  def of[F[_]: Async](
    eventsClient: EventsClient[F],
    offset: Ref[F, Int],
    collectedEvents: Ref[F, List[Event]],
    workerN: Int,
  ): F[EventsExtractorWorker[F]] =
    Slf4jLogger
      .create[F]
      .map(logger => new EventsExtractorWorker[F](eventsClient, offset, collectedEvents, logger)(workerN))
}
