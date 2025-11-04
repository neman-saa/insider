package org.github.insider.polymarket.workers

import cats.effect.{Async, Ref}
import org.github.insider.polymarket.client.TradesClient
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._
import org.github.insider.polymarket.domain.Side.{Buy, Sell}

import scala.collection.immutable.HashMap

private[workers] class TradeExtractorWorker[F[_]: Async](
  offset: Ref[F, Int],
  // (walletId, tokenId) -> (totalSize, totalPrice)
  tradesAggregated: Ref[F, HashMap[(String, String), (BigDecimal, BigDecimal)]],
  id: String,
  tradesClient: TradesClient[F],
  logger: Logger[F]
)(workerNumber: Int) {

  def run(limit: Int, maxOffSet: Option[Int]): F[Unit] = {
    offset.getAndUpdate(_ + limit).flatMap {
      case offset if maxOffSet.isDefined && maxOffSet.get < offset => ().pure[F]
      case offset =>
        for {
          trades <- tradesClient.getTradesHistoryByMarket(id, offset, limit)
          _ <- logger.info(
            s"[Trade worker: $workerNumber] received ${trades.length} trades, current offset: $offset, market id: $id"
          )
          _ <- trades.traverse(trade =>
            tradesAggregated.update { map =>
              val (size, totalPrice) = map.getOrElse((trade.wallet, trade.token.id), (0, 0))
              val sign = trade.side match {
                case Buy  => 1
                case Sell => -1
              }
              val newWallet = (size + trade.size * sign, totalPrice + trade.size * trade.price * sign)
              map + ((trade.wallet, trade.token.id) -> newWallet)
            }
          )
        } yield ()
    }
  }
}

private[workers] object TradeExtractorWorker {
  def of[F[_]: Async](
    offset: Ref[F, Int],
    tradesAggregated: Ref[F, HashMap[(String, String), (BigDecimal, BigDecimal)]],
    id: String,
    tradesClient: TradesClient[F],
    workerNumber: Int
  ): F[TradeExtractorWorker[F]] = {
    Slf4jLogger
      .create[F]
      .map(logger =>
        new TradeExtractorWorker[F](offset: Ref[F, Int], tradesAggregated, id, tradesClient, logger)(workerNumber)
      )
  }
}
