package org.github.insider.alchemy.workers

import cats.data.NonEmptyList
import cats.effect.Ref
import cats.effect.kernel.Async
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._
import org.github.insider.alchemy.client.TransfersClient
import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.processors.TransfersProcessor
import org.github.insider.alchemy.repository.{AggregatedTradesRepository, TradesRepository}
import org.github.insider.polymarket.CTFType
import org.github.insider.polymarket.configs.MainConfig.PolygonContracts

class TradeWorker[F[_]: Async](
  latestProcessedBlockR: Ref[F, Int],
  finalBlock: Int,
  logger: Logger[F],
  client: TransfersClient[F],
  transfersProcessor: TransfersProcessor,
  tradesRepository: TradesRepository[F],
  aggregatedRepository: AggregatedTradesRepository[F],
  polygonContracts: PolygonContracts,
)(step: Int, workerNumber: Int) {
  def run: F[Unit] =
    latestProcessedBlockR.getAndUpdate(_ + step).flatMap { latestProcessedBlock =>
      if (latestProcessedBlock >= finalBlock)
        logger.info(s"[worker-$workerNumber] Finished, no more trades.")
      else
        runRange(latestProcessedBlock + 1, latestProcessedBlock + step) >> run
    }

  private def runRange(fromBlock: Int, toBlock: Int): F[Unit] = {
    for {
      _ <- logger.info(s"[worker-$workerNumber] Starting range $fromBlock - $toBlock")

      standardCtfTransfers <- getAssetsTransfersInRange(fromBlock, toBlock, polygonContracts.standardCtf)
      standardCtfTrades     = transfersProcessor.extractTradesFrom(standardCtfTransfers, CTFType.StandardCTF)
      negRiskCtfTransfers  <- getAssetsTransfersInRange(fromBlock, toBlock, polygonContracts.negRiskCtf)
      negRiskCtfTrades      = transfersProcessor.extractTradesFrom(negRiskCtfTransfers, CTFType.NegRiskCTF)

      trades = standardCtfTrades ++ negRiskCtfTrades

      _ <- logger.info(
        s"[worker-$workerNumber] Standard CTF transfers fetched - ${standardCtfTransfers.size}, trades extracted - ${standardCtfTrades.size}"
      )
      _ <- logger.info(
        s"[worker-$workerNumber] Neg Risk CTF transfers fetched - ${negRiskCtfTransfers.size}, trades extracted - ${negRiskCtfTrades.size}"
      )

      nel = NonEmptyList.fromList(trades)
      _  <- nel.fold(0.pure[F])(tradesRepository.insert)
      _  <- nel.fold(0.pure[F])(aggregatedRepository.insert)

      _ <- logger.info(s"[worker-$workerNumber] Finished range $fromBlock - $toBlock")
    } yield ()
  }

  private def getAssetsTransfersInRange(fromBlock: Int, toBlock: Int, contract: String): F[List[AssetTransfer]] = {
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
      transfersTo   <- rec(Nil, None, Some(contract), None)
      transfersFrom <- rec(Nil, None, None, Some(contract))
      transfers      = transfersTo.reverse ++ transfersFrom.reverse
    } yield transfers
  }
}

object TradeWorker {
  def apply[F[_]: Async](
    fromBlock: Ref[F, Int],
    toBlock: Int,
    transfersClient: TransfersClient[F],
    transfersProcessor: TransfersProcessor,
    tradesRepository: TradesRepository[F],
    aggregatedRepository: AggregatedTradesRepository[F],
    polygonContracts: PolygonContracts,
  )(
    step: Int,
    workerNumber: Int
  ): F[TradeWorker[F]] =
    Slf4jLogger
      .create[F]
      .map(logger =>
        new TradeWorker[F](
          fromBlock,
          toBlock,
          logger,
          transfersClient,
          transfersProcessor,
          tradesRepository,
          aggregatedRepository,
          polygonContracts,
        )(
          step: Int,
          workerNumber
        )
      )
}
