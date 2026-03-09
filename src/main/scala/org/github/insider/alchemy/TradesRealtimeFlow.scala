package org.github.insider.alchemy

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.Ref
import cats.effect.kernel.Async
import cats.syntax.all._
import fs2.concurrent.Topic
import org.github.insider.alchemy.client.TransfersClient
import org.github.insider.alchemy.domain.{AssetTransfer, User}
import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.processors.TransfersProcessor
import org.github.insider.alchemy.repository.{AggregatedTradesRepository, TradesRepository}
import org.github.insider.leaderboard.{HexAddress, Leaderboards}
import org.github.insider.polymarket.configs.MainConfig.AlchemyConfig
import org.github.insider.polymarket.domain.Trade
import org.github.insider.leaderboard.TradeNotification
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

class TradesRealtimeFlow[F[_]: Async: Parallel](
  client: TransfersClient[F],
  transfersProcessor: TransfersProcessor[F],
  tradesRepository: TradesRepository[F],
  aggregatedRepository: AggregatedTradesRepository[F],
  alchemyConfig: AlchemyConfig,
  topic: Topic[F, TradeNotification],
  leaderboards: Leaderboards[F],
)(logger: Logger[F]) {

  def runForever: F[Unit] = {
    def realtimeAction(latestProcessedBlockR: Ref[F, Long]): F[List[Trade]] =
      for {
        latestProcessedBlock <- latestProcessedBlockR.get
        toBlock               = latestProcessedBlock + 100
        transfers            <- getAssetsTransfersInRange(fromBlock = latestProcessedBlock + 1, toBlock = toBlock)
        trades               <- transfersProcessor.extractTradesFrom(transfers)

        notifications <- trades.traverse { trade =>
          leaderboards.find(HexAddress(trade.makerAddress)).map { leaderboardEntries =>
            TradeNotification(trade, leaderboardEntries)
          }
        }

        filteredNotifications = notifications.filter { notification =>
          notification.leaderboardEntries.nonEmpty && notification.trade.singleTokenPrice < BigDecimal(0.9)
        }

        _ <- fs2
          .Stream
          .emits(filteredNotifications)
          .evalMap(topic.publish1)
          .compile
          .drain

        nel = NonEmptyList.fromList(trades)
        _  <- nel.fold(0.pure[F])(tradesRepository.insert)
        _  <- nel.fold(0.pure[F])(aggregatedRepository.insert)

        nextLatestBlock = transfers.map(_.blockNum).maxOption.getOrElse(toBlock)
        _              <- latestProcessedBlockR.set(nextLatestBlock)

        _ <- logger.info(s"Finished range $latestProcessedBlock - $nextLatestBlock, sleeping 3 seconds...")
        _ <- Async[F].sleep(3.seconds)
      } yield trades

    for {
      latestProcessedBlockR <- Ref.empty[F, Long]
      latestProcessedBlock  <- tradesRepository.getLatestBlock
      _                     <- latestProcessedBlockR.set(latestProcessedBlock)
      _ <- fs2
        .Stream
        .repeatEval(realtimeAction(latestProcessedBlockR))
        .compile
        .drain
    } yield ()
  }

  private def getAssetsTransfersInRange(fromBlock: Long, toBlock: Long): F[List[AssetTransfer]] = {
    def rec(
      transfers: List[AssetTransfer],
      page: Option[String],
      toAddress: Option[String],
      fromAddress: Option[String]
    ): F[List[AssetTransfer]] = {
      for {
        resp <- client.getAssetTransfers(
          fromBlock    = Some("0x" + fromBlock.toHexString),
          toBlock      = Some("0x" + toBlock.toHexString),
          fromAddress  = fromAddress,
          toAddress    = toAddress,
          category     = Set(ERC1155, ERC20),
          withMetadata = Some(true),
          page         = page
        )
        updatedTransfers = transfers ++ resp.transfers.flatMap(AssetTransfer.fromTransfer)
        result <- resp.pageKey match {
          case None        => updatedTransfers.pure[F]
          case a @ Some(_) => rec(updatedTransfers, a, toAddress, fromAddress)
        }
      } yield result
    }

    for {
      _             <- logger.info(s"Starting extraction for range $fromBlock - $toBlock")
      transfersTo   <- rec(Nil, None, Some(alchemyConfig.ctfAddress), None)
      transfersFrom <- rec(Nil, None, None, Some(alchemyConfig.ctfAddress))
      transfers      = transfersTo.reverse ++ transfersFrom.reverse
      _ <- logger.info(s"Finished extraction for range $fromBlock - $toBlock with ${transfers.size} transfers")
    } yield transfers
  }
}

object TradesRealtimeFlow {
  def of[F[_]: Async: Parallel](
    transfersClient: TransfersClient[F],
    transfersProcessor: TransfersProcessor[F],
    tradesRepository: TradesRepository[F],
    aggregatedRepository: AggregatedTradesRepository[F],
    alchemyConfig: AlchemyConfig,
    topic: Topic[F, TradeNotification],
    leaderboards: Leaderboards[F],
  ): F[TradesRealtimeFlow[F]] =
    Slf4jLogger
      .create[F]
      .map(logger =>
        new TradesRealtimeFlow[F](
          transfersClient,
          transfersProcessor,
          tradesRepository,
          aggregatedRepository,
          alchemyConfig,
          topic,
          leaderboards
        )(logger)
      )
}
