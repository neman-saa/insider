package org.github.insider.alchemy.workers

import cats.effect.Ref
import cats.effect.kernel.Async
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._
import org.github.insider.alchemy.client.TransfersClient
import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}

class TradeWorker[F[_]: Async](
  fromBlock: Ref[F, Int],
  toBlock: Int,
  logger: Logger[F],
  client: TransfersClient[F],
  ctfAddress: String
)(workerNumber: Int) {

  def run: F[Unit] = for {
    fromBlock <- fromBlock.getAndUpdate(_ + 100)
    _ <-
      if (fromBlock > toBlock) logger.info(s"[worker-$workerNumber] Finished, no more trades.")
      else runRange(fromBlock, 100)
  } yield ()

  def runRange(fromBlock: Int, nBlocks: Int): F[Unit] = {

    def rec(
      transfers: List[AssetTransfer],
      page: Option[String],
      toAddress: Option[String],
      fromAddress: Option[String]
    ): F[List[AssetTransfer]] = for {
      resp <- client.getAssetTransfers(
        fromBlock    = Some(fromBlock.toHexString),
        toBlock      = Some((fromBlock + nBlocks - 1).toHexString),
        fromAddress  = fromAddress,
        toAddress    = toAddress,
        category     = Set(ERC1155, ERC20),
        withMetadata = None,
        page         = page
      )
      transfersAll = transfers ++ resp.transfers.flatMap(AssetTransfer.fromTransfer)
      _ <- resp.page match {
        case None        => transfersAll.pure[F]
        case a @ Some(_) => rec(transfersAll, a, toAddress, fromAddress)
      }
    } yield ()

    for {
      transfersTo   <- rec(Nil, None, None, Some(ctfAddress))
      transfersFrom <- rec(Nil, None, Some(ctfAddress), None)
      trades = (transfersTo ++ transfersFrom)
        .groupBy(_.blockNum)
        .values
        .toList
        .flatMap(AssetTransfer.getTradesFromBlock)
      _ <- ().pure[F] // some trades logic
      _ <- logger.info(s"[worker-$workerNumber] Finished range $fromBlock - ${fromBlock + nBlocks}")
    } yield ()
  }
}

object TradeWorker {
  def apply[F[_]: Async](fromBlock: Ref[F, Int], toBlock: Int, transfersClient: TransfersClient[F], ctfAddress: String)(
    workerNumber: Int
  ): F[TradeWorker[F]] =
    Slf4jLogger
      .create[F]
      .map(logger => new TradeWorker[F](fromBlock, toBlock, logger, transfersClient, ctfAddress)(workerNumber))
}
