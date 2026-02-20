package org.github.insider.alchemy.processors

import cats.effect.kernel.Sync
import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.AssetTransfer.{ERC1155Transfer, USDCTransfer, UnknownTransfer}
import org.github.insider.polymarket.domain.Side.{Buy, Sell}
import org.github.insider.polymarket.domain.Trade
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._

private class TransfersProcessorImpl[F[_]: Sync](logger: Logger[F]) extends TransfersProcessor[F] {

  private val CTFAddress = "0xc5d563a36ae78145c45a50134d48a1215220f80a"

  private val FilterOutAddresses =
    Set(
      "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296", // burn-mint address
      "0x3a3bd7bb9528e159577f7c2e685cc81a765002e2", // collateral address
      "0x0000000000000000000000000000000000000000", // null address
    )

  override def extractTradesFrom(transfers: List[AssetTransfer]): F[List[Trade]] = {
    val transfersGroupedByBlockNum: List[List[AssetTransfer]] =
      transfers
        .groupBy(_.blockNum)
        .values
        .toList

    transfersGroupedByBlockNum.flatTraverse(extractTradesForSingleBlock)
  }

  private def extractTradesForSingleBlock(transfers: List[AssetTransfer]): F[List[Trade]] = {
    def matchTransfers(usdcs: List[USDCTransfer], erc1155s: List[ERC1155Transfer]): F[List[Trade]] = {
      val filteredUsdcs =
        usdcs.filter { usdc =>
          !(FilterOutAddresses.contains(usdc.from) || FilterOutAddresses.contains(usdc.to))
        }
      val filteredErcs = erc1155s.filter { erc1155 =>
        !(FilterOutAddresses.contains(erc1155.from) || FilterOutAddresses.contains(erc1155.to))
      }

      val maybePairedTrades: List[(USDCTransfer, Option[Trade])] = filteredUsdcs.map { usdc =>
        val erc          = filteredErcs.find(x => x.from == usdc.to && x.to == usdc.from)
        val makerAddress = if (usdc.to == CTFAddress) usdc.from else usdc.to
        val side         = if (usdc.from == CTFAddress) Sell else Buy

        usdc -> erc.map(erc =>
          Trade(
            makerAddress = makerAddress,
            tokenId      = BigDecimal(BigInt(erc.tokenId.drop(2), 16)).toString,
            side         = side,
            amount       = BigDecimal(BigInt(erc.value.drop(2), 16)),
            totalPrice   = usdc.value,
            txHash       = erc.hash,
            timestamp    = erc.blockTimestamp,
          )
        )
      }

      val maybeTrades: F[List[Option[Trade]]] = maybePairedTrades.traverse {
        case (_, Some(trade)) =>
          Sync[F].pure(trade.some)
        case (usdc, None) if usdc.value == BigDecimal(0.0000010) => // strange return of usdc, can be ignored
          Sync[F].pure(none)
        case (usdc, None) =>
          logger.info(s"No match found for USDC transfer - $usdc") *> Sync[F].pure(none)
      }

      maybeTrades.map(_.flatten)
    }

    val groupedByHash = transfers.groupBy(_.hash).values.toList

    groupedByHash.flatTraverse { transfers =>
      val (usdcTfs: List[USDCTransfer], erc1155Tfs: List[ERC1155Transfer]) =
        transfers.partition {
          case _: ERC1155Transfer => false
          case _: USDCTransfer    => true
        }

      val unknownTransfers: List[UnknownTransfer] =
        transfers.collect { case transfer: UnknownTransfer => transfer }

      unknownTransfers.traverse(unknownTransfer => logger.info(s"Unknown transfer detected - $unknownTransfer")) *>
        matchTransfers(usdcTfs, erc1155Tfs)
    }
  }
}

object TransfersProcessorImpl {
  def of[F[_]: Sync](): F[TransfersProcessor[F]] =
    Slf4jLogger.create[F].map(logger => new TransfersProcessorImpl[F](logger))
}
