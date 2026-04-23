package org.github.insider.alchemy

import cats.{Applicative, Parallel}
import cats.data.NonEmptyList
import cats.effect.{Clock, Ref}
import cats.effect.implicits.genSpawnOps
import cats.effect.kernel.Async
import cats.syntax.all._
import fs2.concurrent.Topic
import org.github.insider.alchemy.client.TransfersClient
import org.github.insider.alchemy.domain.{AssetTransfer, User}
import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.processors.TransfersProcessor
import org.github.insider.alchemy.repository.{AggregatedTradesRepository, TradesRepository}
import org.github.insider.leaderboard.LeaderboardEntry.{AdvancedLeaderboardEntry, SimpleLeaderboardEntry}
import org.github.insider.leaderboard.{HexAddress, Leaderboards}
import org.github.insider.polymarket.configs.MainConfig.AlchemyConfig
import org.github.insider.polymarket.domain.{Event, Side, Trade}
import org.github.insider.leaderboard.TradeNotification
import org.github.insider.notifications.services.InsiderTelegramBot
import org.github.insider.polymarket.EventsCached
import org.github.insider.realtime.tokens.{TokensInfoRegistry, TokensInfoRepository}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.concurrent.duration.DurationInt

class TradesRealtimeFlow[F[_]: Async: Parallel](
  client: TransfersClient[F],
  transfersProcessor: TransfersProcessor,
  tradesRepository: TradesRepository[F],
  aggregatedRepository: AggregatedTradesRepository[F],
  tokensInfoRepository: TokensInfoRepository[F],
  alchemyConfig: AlchemyConfig,
  tgBot: InsiderTelegramBot[F],
  leaderboards: Leaderboards[F, AdvancedLeaderboardEntry],
  eventsCached: EventsCached[F],
  tokensInfoRegistry: TokensInfoRegistry[F],
)(logger: Logger[F]) {

  def runForever: F[Unit] = {
    def realtimeAction(
      latestProcessedBlockR: Ref[F, Long],
      latestHealthcheckInstantR: Ref[F, Instant],
    ): F[List[Trade]] =
      for {
        latestProcessedBlock <- latestProcessedBlockR.get
        toBlock               = latestProcessedBlock + 100
        transfers            <- getAssetsTransfersInRange(fromBlock = latestProcessedBlock + 1, toBlock = toBlock)
        trades                = transfersProcessor.extractTradesFrom(transfers)

        leaderboard       <- leaderboards.getLeaderboard
        tokensMetaInfo    <- eventsCached.getTokensMetaInfo(trades.map(_.tokenId))
        updatedTokensInfo <- tokensInfoRegistry.updateWith(trades, tokensMetaInfo, leaderboard)

        tradesNel = NonEmptyList.fromList(trades)
        _        <- tradesNel.fold(0.pure[F])(tradesRepository.insert)
        _        <- tradesNel.fold(0.pure[F])(aggregatedRepository.insert)

        tokensInfoNel = NonEmptyList.fromList(updatedTokensInfo)
        _            <- tokensInfoNel.fold(Async[F].unit)(tokensInfoRepository.insert)

        nextLatestBlock = transfers.map(_.blockNum).maxOption.getOrElse(latestProcessedBlock + 1)
        _              <- latestProcessedBlockR.set(nextLatestBlock)

        _ <- appHealthCheckViaTgBot(latestHealthcheckInstantR)

        _ <- logger.info(s"Finished range [${latestProcessedBlock + 1} - $nextLatestBlock], sleeping 3 seconds...")
        _ <- Async[F].sleep(3.seconds)
      } yield trades

    for {
      latestHealthcheckInstantR <- Ref.ofEffect[F, Instant](Clock[F].realTimeInstant)

      latestProcessedBlockR <- Ref.empty[F, Long]
      latestProcessedBlock  <- tradesRepository.getLatestBlock
      _                     <- latestProcessedBlockR.set(latestProcessedBlock)
      _ <- fs2
        .Stream
        .repeatEval(realtimeAction(latestProcessedBlockR, latestHealthcheckInstantR))
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
      _             <- logger.info(s"Starting extraction for range [$fromBlock - $toBlock]")
      transfersTo   <- rec(Nil, None, Some(alchemyConfig.ctfAddress), None)
      transfersFrom <- rec(Nil, None, None, Some(alchemyConfig.ctfAddress))
      transfers      = transfersTo.reverse ++ transfersFrom.reverse
      _ <- logger.info(s"Finished extraction for range [$fromBlock - $toBlock] with ${transfers.size} transfers")
    } yield transfers
  }

  private def appHealthCheckViaTgBot(latestHealthcheckInstantR: Ref[F, Instant]): F[Unit] =
    for {
      now                      <- Clock[F].realTimeInstant
      latestHealthcheckInstant <- latestHealthcheckInstantR.get

      nowSeconds                      = now.getEpochSecond
      latestHealthcheckInstantSeconds = latestHealthcheckInstant.getEpochSecond
      delta                           = nowSeconds - latestHealthcheckInstantSeconds

      _ <- Applicative[F].whenA(delta > 1800)(tgBot.sendAlive >> latestHealthcheckInstantR.set(now))
    } yield ()
}

object TradesRealtimeFlow {
  def of[F[_]: Async: Parallel](
    transfersClient: TransfersClient[F],
    transfersProcessor: TransfersProcessor,
    tradesRepository: TradesRepository[F],
    aggregatedRepository: AggregatedTradesRepository[F],
    tokensInfoRepository: TokensInfoRepository[F],
    alchemyConfig: AlchemyConfig,
    tgBot: InsiderTelegramBot[F],
    leaderboards: Leaderboards[F, AdvancedLeaderboardEntry],
    eventsCached: EventsCached[F],
    tokensInfoRegistry: TokensInfoRegistry[F],
  ): F[TradesRealtimeFlow[F]] =
    Slf4jLogger
      .create[F]
      .map(logger =>
        new TradesRealtimeFlow[F](
          transfersClient,
          transfersProcessor,
          tradesRepository,
          aggregatedRepository,
          tokensInfoRepository,
          alchemyConfig,
          tgBot,
          leaderboards,
          eventsCached,
          tokensInfoRegistry,
        )(logger)
      )
}
