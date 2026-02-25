package org.github.insider.alchemy

import cats.effect.kernel.Sync
import cats.syntax.all._
import org.github.insider.alchemy.client.TransfersClient
import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.processors.TransfersProcessor
import org.github.insider.alchemy.repository.TradesRepository
import org.github.insider.polymarket.configs.MainConfig.AlchemyConfig
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

class TradesFlow[F[_]: Sync](
  client: TransfersClient[F],
  transfersProcessor: TransfersProcessor[F],
  tradesRepository: TradesRepository[F],
  alchemyConfig: AlchemyConfig,
)(logger: Logger[F]) {

  def run(fromBlock: Int, toBlock: Int, batchSize: Int): F[Unit] =
    (fromBlock to toBlock).grouped(batchSize).toList.traverse_ { range =>
      val from = range.start
      val to   = range.end

      for {
        transfers <- getAssetsTransfersInRange(from, to)
        trades    <- transfersProcessor.extractTradesFrom(transfers)
        _         <- logger.info(s"Trades extracted - ${trades.size}")
        _         <- tradesRepository.insert(trades)
        _         <- logger.info(s"Finished range $from - $to}")
      } yield ()
    }

  private def getAssetsTransfersInRange(fromBlock: Int, toBlock: Int): F[List[AssetTransfer]] = {
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

object TradesFlow {
  def of[F[_]: Sync](
    transfersClient: TransfersClient[F],
    transfersProcessor: TransfersProcessor[F],
    tradesRepository: TradesRepository[F],
    alchemyConfig: AlchemyConfig,
  ): F[TradesFlow[F]] =
    Slf4jLogger
      .create[F]
      .map(logger => new TradesFlow[F](transfersClient, transfersProcessor, tradesRepository, alchemyConfig)(logger))
}
