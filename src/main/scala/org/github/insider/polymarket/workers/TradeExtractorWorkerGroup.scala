package org.github.insider.polymarket.workers

import cats.Parallel
import cats.effect.{Async, Ref}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._
import org.github.insider.polymarket.client.TradesClient
import org.github.insider.polymarket.domain.Market
import scala.collection.immutable.HashMap

class TradeExtractorWorkerGroup[F[_]: Async: Parallel](
  logger: Logger[F],
  numberOfWorkers: Int,
  client: TradesClient[F]
) {
  def getAllTradesByMarket(
    limit: Int,
    maxDepth: Option[Int],
    id: String
  ): F[HashMap[(String, String), (BigDecimal, BigDecimal)]] =
    for {
      offset <- Ref.of[F, Int](0)
      tradesAggregated <- Ref.of[F, HashMap[(String, String), (BigDecimal, BigDecimal)]](
        HashMap.empty
      )
      workers <- (1 to numberOfWorkers)
        .toList
        .traverse(i => TradeExtractorWorker.of[F](offset, tradesAggregated, id, client, i))
      _   <- workers.parTraverse(_.run(limit, maxDepth))
      res <- tradesAggregated.get
    } yield res

  def tradesByAllMarkets(
    markets: List[Market],
    maxDepth: Option[Int],
    limit: Int
  ): F[List[(Market, HashMap[(String, String), (BigDecimal, BigDecimal)])]] =
    markets
      .traverse(market => getAllTradesByMarket(limit, maxDepth, market.conditionId).map((market, _)))
      .map(_.filter(_._2.exists(filterMarket)))

  def filterMarket(tradeSum: ((String, String), (BigDecimal, BigDecimal))): Boolean = {
    val (totalSize, totalPrice) = tradeSum._2
    totalSize > 100000 || totalPrice > 50000
  }

}

object TradeExtractorWorkerGroup {
  def of[F[_]: Async: Parallel](nWorkers: Int, client: TradesClient[F]): F[TradeExtractorWorkerGroup[F]] =
    Slf4jLogger.create[F].map(logger => new TradeExtractorWorkerGroup[F](logger, nWorkers, client))
}
