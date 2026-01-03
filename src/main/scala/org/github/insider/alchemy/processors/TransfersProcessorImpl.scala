package org.github.insider.alchemy.processors

import org.github.insider.alchemy.domain.AssetTransfer
import org.github.insider.alchemy.domain.AssetTransfer.{ERC1155Transfer, USDCTransfer}
import org.github.insider.polymarket.domain.Trade

private class TransfersProcessorImpl(
  ctfAddress: String,
  burnMintAddress: String
) extends TransfersProcessor {

  override def extractTradesFrom(transfers: List[AssetTransfer]): List[Trade] = {
    val transfersGroupedByBlockNum: List[List[AssetTransfer]] =
      transfers
        .groupBy(_.blockNum)
        .values
        .toList

    transfersGroupedByBlockNum.flatMap(extractTradesForSingleBlock)
  }

  private def extractTradesForSingleBlock(transfers: List[AssetTransfer]): List[Trade] = {
    def matchTransfers(usdcs: List[USDCTransfer], erc1155s: List[ERC1155Transfer]): List[Trade] =
      if (erc1155s.groupBy(_.tokenId).size == 1) {
        if (usdcs.count(_.to == ctfAddress) == 1) {
          val buyerAddress  = usdcs.filter(_.to == ctfAddress).head.from
          val assetId       = erc1155s.head.tokenId
          val buySum        = usdcs.filter(_.to == ctfAddress).head.value
          val sellersValues = erc1155s.filter(_.to == ctfAddress)
          val tokensSum     = BigDecimal(BigInt(erc1155s.filter(_.from == ctfAddress).head.value.drop(2), 16))
          val trades = sellersValues.map(erc1155 =>
            Trade(
              erc1155.from,
              buyerAddress,
              assetId,
              BigDecimal(BigInt(erc1155.value.drop(2), 16)),
              tokensSum / buySum,
              erc1155.blockTimestamp,
              erc1155.hash
            )
          )
          trades
        } // makers = sellers
        else {
          val sellerAddress = usdcs.filter(_.from == ctfAddress).head.from
          val assetId       = erc1155s.head.tokenId
          val buySum        = usdcs.filter(_.from == ctfAddress).head.value
          val buyers        = erc1155s.filter(_.from == ctfAddress)
          val tokensSum     = BigDecimal(BigInt(erc1155s.filter(_.to == ctfAddress).head.value.drop(2), 16))
          val trades = buyers.map(erc1155 =>
            Trade(
              sellerAddress,
              erc1155.to,
              assetId,
              BigDecimal(BigInt(erc1155.value.drop(2), 16)),
              tokensSum / buySum,
              erc1155.blockTimestamp,
              erc1155.hash
            )
          )
          trades
        } // makers = buyers
      } // trading
      else {
        val usdcsf    = usdcs.filter(t => t.from != burnMintAddress && t.to != burnMintAddress)
        val erc1155sf = erc1155s.filter(t => t.from != burnMintAddress && t.to != burnMintAddress)
        val trades = erc1155sf.map(erc1155 =>
          Trade(
            erc1155.from,
            erc1155.to,
            erc1155.tokenId,
            BigDecimal(BigInt(erc1155.value.drop(2), 16)),
            BigDecimal(BigInt(erc1155.value.drop(2), 16)) / usdcsf
              .filter(x => x.from == erc1155.to && x.to == erc1155.from)
              .head
              .value,
            erc1155.blockTimestamp,
            erc1155.hash
          )
        )
        trades
      } // mint/burn

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
  def apply(ctf: String, burnMint: String): TransfersProcessor =
    new TransfersProcessorImpl(ctf, burnMint)
}
