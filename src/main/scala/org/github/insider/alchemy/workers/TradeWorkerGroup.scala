package org.github.insider.alchemy.workers

import cats.Parallel
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.alchemy.client.TransfersClient
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class TradeWorkerGroup[F[_]: Async: Parallel](
  logger: Logger[F],
  client: TransfersClient[F],
  ctfAddress: String
)(nWorkers: Int) {
  def run(fromBlock: Int, toBlock: Int): F[Unit] = for {
    _         <- logger.info("Application started parsing trades")
    fromBlock <- Ref.of[F, Int](fromBlock)
    workers   <- (1 to nWorkers).toList.traverse(i => TradeWorker(fromBlock, toBlock, client, ctfAddress)(i))
    _         <- workers.parTraverse_(_.run)
    _         <- logger.info("Application finished parsing trades")
  } yield ()
}

object TradeWorkerGroup {
  def apply[F[_]: Async: Parallel](
    client: TransfersClient[F],
    ctfAddress: String
  )(nWorkers: Int): F[TradeWorkerGroup[F]] =
    Slf4jLogger.create[F].map(logger => new TradeWorkerGroup(logger, client, ctfAddress)(nWorkers))
}
