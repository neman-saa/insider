package org.github.insider.alchemy.workers

import cats.effect.Ref
import cats.effect.kernel.Async
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._
import org.github.insider.alchemy.client.TransfersClient
import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.processors.TransfersProcessor
import org.github.insider.alchemy.repository.TradesRepository

class TradeWorker[F[_]: Async](
  fromBlock: Ref[F, Int],
  toBlock: Int,
  logger: Logger[F],
  client: TransfersClient[F],
  ctfAddress: String,
  step: Int,
  transfersProcessor: TransfersProcessor[F],
  tradesRepository: TradesRepository[F],
)(workerNumber: Int) {
  def run: F[Unit] =
    fromBlock.getAndUpdate(_ + step).flatMap { fromBlock =>
      if (fromBlock > toBlock)
        logger.info(s"[worker-$workerNumber] Finished, no more trades.")
      else
        runRange(fromBlock, Math.min(step - 1, toBlock - fromBlock)) >> run
    }

  private def runRange(fromBlock: Int, nBlocks: Int): F[Unit] = {

    def rec(
      transfers: List[AssetTransfer],
      page: Option[String],
      toAddress: Option[String],
      fromAddress: Option[String]
    ): F[List[AssetTransfer]] = for {
      resp <- client.getAssetTransfers(
        fromBlock    = Some("0x" + fromBlock.toHexString),
        toBlock      = Some("0x" + (fromBlock + nBlocks).toHexString),
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

    for {
      _             <- logger.info(s"[worker-$workerNumber] Starting range $fromBlock - ${fromBlock + nBlocks}")
      transfersTo   <- rec(Nil, None, Some(ctfAddress), None)
      transfersFrom <- rec(Nil, None, None, Some(ctfAddress))
      transfers      = transfersTo ++ transfersFrom
      trades        <- transfersProcessor.extractTradesFrom(transfers)
      _ <- logger.info(
        s"[worker-$workerNumber] Transfers fetched - ${transfers.size}, trades extracted - ${trades.size}"
      )
      _ <- tradesRepository.insert(trades)
      _ <- logger.info(s"[worker-$workerNumber] Finished range $fromBlock - ${fromBlock + nBlocks}")
    } yield ()
  }
}

object TradeWorker {
  def apply[F[_]: Async](
    fromBlock: Ref[F, Int],
    toBlock: Int,
    transfersClient: TransfersClient[F],
    ctfAddress: String,
    step: Int,
    transfersProcessor: TransfersProcessor[F],
    tradesRepository: TradesRepository[F],
  )(
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
          ctfAddress,
          step,
          transfersProcessor,
          tradesRepository
        )(
          workerNumber
        )
      )
}
