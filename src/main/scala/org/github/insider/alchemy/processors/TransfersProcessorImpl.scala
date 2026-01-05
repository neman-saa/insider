package org.github.insider.alchemy.processors

import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.AssetTransfer.{ERC1155Transfer, USDCTransfer}
import org.github.insider.polymarket.domain.Side.{Buy, Sell}
import org.github.insider.polymarket.domain.Trade

private class TransfersProcessorImpl(
  ctfAddress: String,
  burnMintAddress: String,
  collateral: String
) extends TransfersProcessor {

  private val nullAddress = "0x0000000000000000000000000000000000000000"
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
      val filterdUsds = usdcs.filter(x =>
        x.from != burnMintAddress && x.from != collateral && x.from != nullAddress &&
          x.to != burnMintAddress && x.to != collateral && x.to != nullAddress
      )
      val filterdErcs = erc1155s.filter(x =>
        x.from != burnMintAddress && x.from != collateral && x.from != nullAddress &&
          x.to != burnMintAddress && x.to != collateral && x.to != nullAddress
      )

      val trades = filterdUsds.flatMap{ usdc =>
        val erc = filterdErcs.find(x => x.from == usdc.to && x.to == usdc.from)
        val makerAddress = if(usdc.to == ctfAddress) usdc.from else usdc.to
        val side = if(usdc.from == ctfAddress) Sell else Buy
        erc.map(erc => Trade(
          makerAddress,
          erc.tokenId,
          BigDecimal(BigInt(erc.value.drop(2), 16)),
          usdc.value,
          erc.blockTimestamp,
          erc.hash,
          side
        ))
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
  def apply(ctf: String, burnMint: String, collateral: String): TransfersProcessor =
    new TransfersProcessorImpl(ctf, burnMint, collateral)
}
