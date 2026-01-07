package org.github.insider.alchemy.processors

import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.AssetTransfer.{ERC1155Transfer, USDCTransfer}
import org.github.insider.polymarket.domain.Side.{Buy, Sell}
import org.github.insider.polymarket.domain.Trade

private class TransfersProcessorImpl extends TransfersProcessor {

  private val CTFAddress = "0xc5d563a36ae78145c45a50134d48a1215220f80a"

  private val FilterOutAddresses =
    Set(
      "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296", // burn-mint address
      "0x3a3bd7bb9528e159577f7c2e685cc81a765002e2", // collateral address
      "0x0000000000000000000000000000000000000000", // null address
    )

  private val scale = 1000000

  override def extractTradesFrom(transfers: List[AssetTransfer]): List[Trade] = {
    val transfersGroupedByBlockNum: List[List[AssetTransfer]] =
      transfers
        .groupBy(_.blockNum)
        .values
        .toList

    transfersGroupedByBlockNum.flatMap(extractTradesForSingleBlock)
  }

  private def extractTradesForSingleBlock(transfers: List[AssetTransfer]): List[Trade] = {
    def matchTransfers(usdcs: List[USDCTransfer], erc1155s: List[ERC1155Transfer]): List[Trade] = {
      val filteredUsds =
        usdcs.filter { usdc =>
          !(FilterOutAddresses.contains(usdc.from) || FilterOutAddresses.contains(usdc.to))
        }
      val filteredErcs = erc1155s.filter { erc1155 =>
        !(FilterOutAddresses.contains(erc1155.from) || FilterOutAddresses.contains(erc1155.to))
      }

      val trades = filteredUsds.flatMap { usdc =>
        val erc          = filteredErcs.find(x => x.from == usdc.to && x.to == usdc.from)
        val makerAddress = if (usdc.to == CTFAddress) usdc.from else usdc.to
        val side         = if (usdc.from == CTFAddress) Sell else Buy

        erc.map(erc =>
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

      trades
    }

    val groupedByHash = transfers.groupBy(_.hash).values.toList
    val trades = groupedByHash.flatMap { list =>
      val (usdcTfs: List[USDCTransfer], erc1155Tfs: List[ERC1155Transfer]) =
        list.partition {
          case _: ERC1155Transfer => false
          case _: USDCTransfer    => true
        }
      matchTransfers(usdcTfs, erc1155Tfs)
    }

    trades
  }
}

object TransfersProcessorImpl {
  def apply(): TransfersProcessor = new TransfersProcessorImpl()
}
