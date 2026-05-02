package org.github.insider.alchemy

import cats.{Applicative, Parallel}
import cats.data.NonEmptyList
import cats.effect.{Clock, Ref}
import cats.effect.kernel.Async
import cats.syntax.all._
import org.github.insider.alchemy.client.TransfersClient
import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.processors.TransfersProcessor
import org.github.insider.alchemy.repository.{AggregatedTradesRepository, TradesRepository}
import org.github.insider.leaderboard.LeaderboardEntry.AdvancedLeaderboardEntry
import org.github.insider.leaderboard.Leaderboards
import org.github.insider.polymarket.configs.MainConfig.PolygonContracts
import org.github.insider.notifications.services.InsiderTelegramBot
import org.github.insider.polymarket.EventsCached
import org.github.insider.realtime.tokens.{TokensInfoRegistry, TokensInfoRepository}
import org.github.insider.realtime.wallets.Wallet
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant
import scala.concurrent.duration.DurationInt

class TradesRealtimeFlow[F[_]: Async](
  client: TransfersClient[F],
  transfersProcessor: TransfersProcessor,
  tradesRepository: TradesRepository[F],
  aggregatedRepository: AggregatedTradesRepository[F],
  tokensInfoRepository: TokensInfoRepository[F],
  tgBot: InsiderTelegramBot[F],
  leaderboards: Leaderboards[F, AdvancedLeaderboardEntry],
  eventsCached: EventsCached[F],
  tokensInfoRegistry: TokensInfoRegistry[F],
  polygonContracts: PolygonContracts,
  wallet: Wallet[F]
)(logger: Logger[F]) {

  def runForever: F[Unit] = {
    def realtimeAction(
      latestProcessedBlockR: Ref[F, Long],
      latestHealthCheckInstantR: Ref[F, Instant],
    ): F[Unit] =
      for {
        latestProcessedBlock <- latestProcessedBlockR.get
        toBlock               = latestProcessedBlock + 100

        standardCtfTransfers <- getAssetsTransfersInRange(
          fromBlock = latestProcessedBlock + 1,
          toBlock   = toBlock,
          contract  = polygonContracts.standardCtf,
        )
        negRiskCtfTransfers <- getAssetsTransfersInRange(
          fromBlock = latestProcessedBlock + 1,
          toBlock   = toBlock,
          contract  = polygonContracts.negRiskCtf,
        )
        standardCtfTrades = transfersProcessor.extractTradesFrom(standardCtfTransfers)
        negRiskCtfTrades  = transfersProcessor.extractTradesFrom(negRiskCtfTransfers)

        _ <- logger.info(
          s"CTF transfers fetched - ${standardCtfTransfers.size}, trades extracted - ${standardCtfTrades.size}"
        )
        _ <- logger.info(
          s"Neg Risk CTF transfers fetched - ${negRiskCtfTransfers.size}, trades extracted - ${negRiskCtfTrades.size}"
        )

        trades = (standardCtfTrades ++ negRiskCtfTrades)
          .filter(trade => trade.singleTokenPrice > 0.03 && trade.singleTokenPrice < 0.97)
          .filterNot(_.makerAddress.toLowerCase == "0xa1db359bd1ea4f98eb4cc76e2c37197b3faf594c")

        leaderboard       <- leaderboards.getLeaderboard
        tokensMetaInfo    <- eventsCached.getTokensMetaInfo(trades.map(_.tokenId))
        updatedTokensInfo <- tokensInfoRegistry.updateWith(trades, tokensMetaInfo, leaderboard)

        tradesNel = NonEmptyList.fromList(trades)
        _        <- tradesNel.fold(0.pure[F])(tradesRepository.insert)
        _        <- tradesNel.fold(0.pure[F])(aggregatedRepository.insert)

        tokensInfoNel = NonEmptyList.fromList(updatedTokensInfo)
        _            <- tokensInfoNel.fold(Async[F].unit)(tokensInfoRepository.insert)
        _ <- wallet.performOperations()
        nextLatestBlock = trades.map(_.blockNum).maxOption.getOrElse(latestProcessedBlock + 1)
        _              <- latestProcessedBlockR.set(nextLatestBlock)

        _ <- appHealthCheckViaTgBot(latestHealthCheckInstantR)

        _ <- logger.info(s"Finished range [${latestProcessedBlock + 1} - $nextLatestBlock], sleeping 3 seconds...")
        _ <- Async[F].sleep(3.seconds)
      } yield ()

    for {
      latestHealthCheckInstantR <- Ref.ofEffect[F, Instant](Clock[F].realTimeInstant)

      latestProcessedBlockR <- Ref.empty[F, Long]
      latestProcessedBlock  <- tradesRepository.getLatestBlock
      _                     <- latestProcessedBlockR.set(latestProcessedBlock)

      repeatableAction =
        realtimeAction(latestProcessedBlockR, latestHealthCheckInstantR).handleErrorWith { e =>
          logger.error(s"Error in trades realtime flow: ${e.getMessage}") >> Async[F].sleep(10.seconds)
        }

      _ <- fs2.Stream.repeatEval(repeatableAction).compile.drain
    } yield ()
  }

  private def getAssetsTransfersInRange(fromBlock: Long, toBlock: Long, contract: String): F[List[AssetTransfer]] = {
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
      transfersTo   <- rec(Nil, None, Some(contract), None)
      transfersFrom <- rec(Nil, None, None, Some(contract))
      transfers      = transfersTo.reverse ++ transfersFrom.reverse
      _ <- logger.info(s"Finished extraction for range [$fromBlock - $toBlock] with ${transfers.size} transfers")
    } yield transfers
  }

  private def appHealthCheckViaTgBot(latestHealthCheckInstantR: Ref[F, Instant]): F[Unit] =
    for {
      now                      <- Clock[F].realTimeInstant
      latestHealthCheckInstant <- latestHealthCheckInstantR.get

      nowSeconds                      = now.getEpochSecond
      latestHealthCheckInstantSeconds = latestHealthCheckInstant.getEpochSecond
      delta                           = nowSeconds - latestHealthCheckInstantSeconds

      _ <- Applicative[F].whenA(delta > 3600)(tgBot.sendAlive >> latestHealthCheckInstantR.set(now))
    } yield ()
}

object TradesRealtimeFlow {
  def of[F[_]: Async: Parallel](
    transfersClient: TransfersClient[F],
    transfersProcessor: TransfersProcessor,
    tradesRepository: TradesRepository[F],
    aggregatedRepository: AggregatedTradesRepository[F],
    tokensInfoRepository: TokensInfoRepository[F],
    tgBot: InsiderTelegramBot[F],
    leaderboards: Leaderboards[F, AdvancedLeaderboardEntry],
    eventsCached: EventsCached[F],
    tokensInfoRegistry: TokensInfoRegistry[F],
    polygonContracts: PolygonContracts,
    wallet: Wallet[F]
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
          tgBot,
          leaderboards,
          eventsCached,
          tokensInfoRegistry,
          polygonContracts,
          wallet
        )(logger)
      )
}
