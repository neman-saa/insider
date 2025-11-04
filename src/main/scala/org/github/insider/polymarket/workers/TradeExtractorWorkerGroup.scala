package org.github.insider.polymarket.workers

import cats.Parallel
import cats.effect.{Async, Ref}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._
import org.github.insider.polymarket.client.TradesClient
import org.github.insider.polymarket.persistance.Markets

import scala.collection.immutable.HashMap

class TradeExtractorWorkerGroup[F[_]: Async: Parallel](
  logger: Logger[F],
  numberOfWorkers: Int,
  tradesAggregated: Ref[F, HashMap[(String, String), (BigDecimal, BigDecimal)]],
  client: TradesClient[F],
  markets: Markets[F]
) {
  def getAllTradesByMarket(limit: Int, maxDepth: Option[Int], id: String): F[HashMap[String, (BigDecimal, BigDecimal)]] =
    for {
      offset <- Ref.of[F, Int](0)
      workers <- (1 to numberOfWorkers)
        .toList
        .traverse(i => TradeExtractorWorker.of[F](offset, tradesAggregated, id, client, i))
      _   <- workers.parTraverse(_.run(limit, maxDepth))
      res <- tradesAggregated.get
    } yield res

}

object TradeExtractorWorkerGroup {
  def of[F[_]: Async: Parallel](nWorkers: Int, client: TradesClient[F]): F[TradeExtractorWorkerGroup[F]] =
    for {
      offsetRef <- Ref.of[F, Int](0)
      tradesAggregatedRef <- Ref.of[F, HashMap[(String, String), (BigDecimal, BigDecimal)]](
        HashMap.empty
      )
      logger <- Slf4jLogger.create[F]
    } yield new TradeExtractorWorkerGroup[F](logger, nWorkers, tradesAggregatedRef, client)
}
