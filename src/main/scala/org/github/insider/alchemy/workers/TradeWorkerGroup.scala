package org.github.insider.alchemy.workers

import cats.Parallel
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.alchemy.client.TransfersClient
import org.github.insider.alchemy.processors.TransfersProcessor
import org.github.insider.alchemy.repository.{AggregatedTradesRepository, TradesRepository}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class TradeWorkerGroup[F[_]: Async: Parallel](
  logger: Logger[F],
  client: TransfersClient[F],
  transfersProcessor: TransfersProcessor,
  tradesRepository: TradesRepository[F],
  aggregatedRepository: AggregatedTradesRepository[F],
  ctfAddress: String,
  step: Int
)(nWorkers: Int) {
  def run(fromBlock: Int, toBlock: Int): F[Unit] =
    for {
      _         <- logger.info(s"Starting trades extraction in range: $fromBlock to $toBlock")
      fromBlock <- Ref.of[F, Int](fromBlock)
      workers <-
        (1 to nWorkers)
          .toList
          .traverse(i =>
            TradeWorker(
              fromBlock,
              toBlock,
              client,
              ctfAddress,
              step,
              transfersProcessor,
              tradesRepository,
              aggregatedRepository
            )(i)
          )
      _ <- workers.parTraverse_(_.run)
      _ <- logger.info("Application finished trades extraction")
    } yield ()
}

object TradeWorkerGroup {
  def of[F[_]: Async: Parallel](
    client: TransfersClient[F],
    transfersProcessor: TransfersProcessor,
    tradesRepository: TradesRepository[F],
    aggregatedRepository: AggregatedTradesRepository[F],
    ctfAddress: String,
    step: Int
  )(nWorkers: Int): F[TradeWorkerGroup[F]] =
    Slf4jLogger
      .create[F]
      .map(logger =>
        new TradeWorkerGroup(
          logger,
          client,
          transfersProcessor,
          tradesRepository,
          aggregatedRepository,
          ctfAddress,
          step
        )(nWorkers)
      )
}
