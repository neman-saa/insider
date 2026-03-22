package org.github.insider.alchemy.processors

import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.AssetTransfer.{ERC1155Transfer, USDCTransfer, UnknownTransfer}
import org.github.insider.polymarket.domain.Side.{Buy, Sell}
import org.github.insider.polymarket.domain.Trade
import cats.syntax.all._

import scala.annotation.tailrec

private class TransfersProcessorImpl extends TransfersProcessor {

  private val CTFAddress = "0xc5d563a36ae78145c45a50134d48a1215220f80a"

  private val FilterOutAddresses =
    Set(
      "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296", // burn-mint address
      "0x3a3bd7bb9528e159577f7c2e685cc81a765002e2", // collateral address
      "0x0000000000000000000000000000000000000000", // null address
    )

  override def extractTradesFrom(transfers: List[AssetTransfer]): List[Trade] = {
    val transfersGroupedByBlockNum = transfers.groupBy(_.blockNum)

    val orderedTransfersGroupedByBlockNum =
      transfersGroupedByBlockNum.toList.sortBy { case (blockNum, _) => blockNum }

    orderedTransfersGroupedByBlockNum.flatMap {
      case (blockNum, transfers) => extractTradesForSingleBlock(transfers, blockNum)
    }
  }

  private def extractTradesForSingleBlock(transfers: List[AssetTransfer], blockNum: Long): List[Trade] = {

    /**
      * @param usdcs
      *   list of USDC transfers from/to user wallet address to/from CTF Exchange
      * @param erc1155s
      *   list of ERC1155 transfers from/to CTF Exchange to/from user wallet address
      */
    def matchTransfers(usdcs: List[USDCTransfer], erc1155s: List[ERC1155Transfer], txIndex: Int): List[Trade] = {

      @tailrec
      def constructTrades(
        usdcIndex: Int,
        ercs: List[ERC1155Transfer],
        trades: List[Trade],
      ): List[Trade] = {
        usdcs.get(usdcIndex) match {
          case None => trades
          case Some(usdc) =>
            val maybeTradeWithErcIndex: Option[(Trade, Int)] =
              ercs
                .zipWithIndex
                .find {
                  case (erc, _) => erc.from == usdc.to && erc.to == usdc.from
                }
                .map {
                  case (erc, ercIndex) =>
                    val makerAddress = if (usdc.to == CTFAddress) usdc.from else usdc.to
                    val side         = if (usdc.from == CTFAddress) Sell else Buy

                    Trade(
                      makerAddress   = makerAddress,
                      tokenId        = BigDecimal(BigInt(erc.tokenId.stripPrefix("0x"), 16)).toString,
                      side           = side,
                      amount         = BigDecimal(BigInt(erc.value.stripPrefix("0x"), 16)),
                      totalPrice     = usdc.value,
                      blockNum       = blockNum,
                      txHash         = erc.hash,
                      txIndex        = txIndex,
                      blockTimestamp = erc.blockTimestamp,
                    ) -> ercIndex
                }

            maybeTradeWithErcIndex match {
              case Some((trade, ercIndex)) =>
                constructTrades(
                  usdcIndex = usdcIndex + 1,
                  ercs      = ercs.zipWithIndex.collect { case (transfer, i) if i != ercIndex => transfer },
                  trades    = trades.appended(trade),
                )
              case None =>
                constructTrades(
                  usdcIndex = usdcIndex + 1,
                  ercs      = ercs,
                  trades    = trades,
                )
            }
        }
      }

      val trades: List[Trade] = constructTrades(0, erc1155s, Nil)

      // Sometimes transactions contain same operations separately, we need to group them into one trade
      val combinedTrades: List[Trade] =
        trades
          .groupMapReduce(trade =>
            (trade.makerAddress, trade.tokenId, trade.side, trade.blockNum, trade.txHash, trade.txIndex)
          )(identity) {
            case (trade1, trade2) =>
              trade1.copy(amount = trade1.amount + trade2.amount, totalPrice = trade1.totalPrice + trade2.totalPrice)
          }
          .values
          .toList

      combinedTrades
    }

    /* Map which represents an ordering index of particular tx hash */
    val txsOrdering: Map[String, Int] = transfers.map(_.hash).distinct.zipWithIndex.toMap

    val transfersGroupedByTxHash = transfers.groupBy(_.hash).toList

    val orderedTransfersGroupedByTxIndex =
      transfersGroupedByTxHash.map { case (txHash, transfers) => txsOrdering(txHash) -> transfers }.sortBy {
        case (txIndex, _) => txIndex
      }

    orderedTransfersGroupedByTxIndex.flatMap {
      case (txIndex, transfers) =>
        val (usdcTfs: List[USDCTransfer], erc1155Tfs: List[ERC1155Transfer]) =
          transfers.partition {
            case _: ERC1155Transfer => false
            case _: USDCTransfer    => true
          }

        val filteredUsdcs =
          usdcTfs.filter { usdc =>
            !(FilterOutAddresses.contains(usdc.from) || FilterOutAddresses.contains(usdc.to))
          }
        val filteredErcs = erc1155Tfs.filter { erc1155 =>
          !(FilterOutAddresses.contains(erc1155.from) || FilterOutAddresses.contains(erc1155.to))
        }

        matchTransfers(filteredUsdcs, filteredErcs, txIndex)
    }
  }
}

object TransfersProcessorImpl {
  def apply(): TransfersProcessor = new TransfersProcessorImpl
}
