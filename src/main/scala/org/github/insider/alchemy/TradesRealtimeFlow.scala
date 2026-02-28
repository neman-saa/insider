package org.github.insider.alchemy

import cats.effect.Ref
import cats.effect.kernel.Async
import cats.syntax.all._
import org.github.insider.alchemy.client.TransfersClient
import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.processors.TransfersProcessor
import org.github.insider.alchemy.repository.TradesRepository
import org.github.insider.polymarket.configs.MainConfig.AlchemyConfig
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

class TradesRealtimeFlow[F[_]: Async](
  client: TransfersClient[F],
  transfersProcessor: TransfersProcessor[F],
  tradesRepository: TradesRepository[F],
  alchemyConfig: AlchemyConfig,
)(logger: Logger[F]) {

  def runForever: F[Unit] = {
    def realtimeAction(latestProcessedBlockR: Ref[F, Long]): F[Unit] =
      for {
        latestProcessedBlock <- latestProcessedBlockR.get
        toBlock               = latestProcessedBlock + 1000
        transfers            <- getAssetsTransfersInRange(fromBlock = latestProcessedBlock + 1, toBlock = toBlock)
        trades               <- transfersProcessor.extractTradesFrom(transfers)
        _                    <- logger.info(s"Trades extracted - ${trades.size}")

        _ <- tradesRepository.insert(trades)

        nextLatestBlock = transfers.map(_.blockNum).maxOption.getOrElse(toBlock)
        _              <- latestProcessedBlockR.set(nextLatestBlock)

        _ <- logger.info(s"Finished range $latestProcessedBlock - $nextLatestBlock, sleeping 3 seconds...")
        _ <- Async[F].sleep(3.seconds)
      } yield ()

    for {
      latestProcessedBlockR <- Ref.empty[F, Long]
      latestProcessedBlock  <- tradesRepository.getLatestBlock
      _                     <- latestProcessedBlockR.set(latestProcessedBlock)
      _                     <- fs2.Stream.repeatEval(realtimeAction(latestProcessedBlockR)).compile.drain
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
      _             <- logger.info(s"Starting range $fromBlock - $toBlock")
      transfersTo   <- rec(Nil, None, Some(alchemyConfig.ctfAddress), None)
      transfersFrom <- rec(Nil, None, None, Some(alchemyConfig.ctfAddress))
      transfers      = transfersTo ++ transfersFrom
      _             <- logger.info(s"Transfers fetched - ${transfers.size}")
    } yield transfers
  }
}

object TradesRealtimeFlow {
  def of[F[_]: Async](
    transfersClient: TransfersClient[F],
    transfersProcessor: TransfersProcessor[F],
    tradesRepository: TradesRepository[F],
    alchemyConfig: AlchemyConfig,
  ): F[TradesRealtimeFlow[F]] =
    Slf4jLogger
      .create[F]
      .map(logger =>
        new TradesRealtimeFlow[F](transfersClient, transfersProcessor, tradesRepository, alchemyConfig)(logger)
      )
}
