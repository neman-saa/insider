package org.github.insider.alchemy.domain

import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.domain.dto.{TokenCategory, Transfer}
import org.github.insider.polymarket.domain.Trade

sealed trait AssetTransfer {
  def blockNum: String

  def from: String // Make Address value class

  def to: String

  def hash: String

  def blockTimestamp: Option[String]
}

object AssetTransfer {

  /* Converts DTO into domain model */
  def fromTransfer(transfer: Transfer): Option[AssetTransfer] =
    transfer.category match {
      case Some(ERC20) =>
        for {
          blockNum <- transfer.blockNum
          from     <- transfer.from
          to       <- transfer.to
          hash     <- transfer.hash
          value    <- transfer.value
        } yield USDCTransfer(
          blockNum       = blockNum,
          from           = from,
          to             = to,
          hash           = hash,
          blockTimestamp = transfer.metadata.flatMap(_.blockTimestamp),
          value          = value,
        )

      case Some(ERC1155) =>
        for {
          blockNum <- transfer.blockNum
          from     <- transfer.from
          to       <- transfer.to
          hash     <- transfer.hash
          value    <- transfer.erc1155Metadata.headOption.flatMap(_.value)
          tokenId  <- transfer.erc1155Metadata.headOption.flatMap(_.tokenId)
        } yield ERC1155Transfer(
          blockNum       = blockNum,
          from           = from,
          to             = to,
          hash           = hash,
          blockTimestamp = transfer.metadata.flatMap(_.blockTimestamp),
          value          = value,
          tokenId        = tokenId
        )
      case _ =>
        for {
          blockNum <- transfer.blockNum
          from     <- transfer.from
          to       <- transfer.to
          hash     <- transfer.hash
        } yield UnknownTransfer(
          blockNum       = blockNum,
          from           = from,
          to             = to,
          hash           = hash,
          blockTimestamp = transfer.metadata.flatMap(_.blockTimestamp),
          category       = transfer.category,
        )
    }

  final case class ERC1155Transfer(
    blockNum: String,
    from: String,
    to: String,
    hash: String,
    blockTimestamp: Option[String],
    // ERC1155 transfer metadata
    tokenId: String,
    value: String,
  ) extends AssetTransfer

  final case class USDCTransfer(
    blockNum: String,
    from: String,
    to: String,
    hash: String,
    blockTimestamp: Option[String],
    // ERC20 transfer metadata
    value: BigDecimal,
  ) extends AssetTransfer

  final case class UnknownTransfer(
    blockNum: String,
    from: String,
    to: String,
    hash: String,
    blockTimestamp: Option[String],
    // Unknown asset category
    category: Option[TokenCategory],
  ) extends AssetTransfer

  def getTradesFromBlock(transfers: List[AssetTransfer], ctfAddress: String, wtf: String): List[Trade] = {

    def matchTransfers(usdcs: List[USDCTransfer], erc1155s: List[ERC1155Transfer]): List[Trade] =
      if (erc1155s.groupBy(_.tokenId).size == 1) {
        if (usdcs.count(_.to.contains(ctfAddress)) == 1) {
          val buyerAddress  = usdcs.filter(_.to.contains(ctfAddress)).head.from
          val assetId       = erc1155s.head.tokenId
          val buySum        = usdcs.filter(_.to.contains(ctfAddress)).head.value
          val sellersValues = erc1155s.filter(_.to.contains(ctfAddress))
          val tokensSum     = sellersValues.map(t => BigDecimal(BigInt(t.value.drop(2), 16))).sum
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
          val sellerAddress = usdcs.filter(_.from.contains(ctfAddress)).head.from
          val assetId       = erc1155s.head.tokenId
          val buySum        = usdcs.filter(_.from.contains(ctfAddress)).head.value
          val buyers        = erc1155s.filter(_.from.contains(ctfAddress))
          val tokensSum = BigDecimal(
            BigInt(
              erc1155s
                .filter(_.to.contains(ctfAddress))
                .head
                .value
                .drop(2),
              16
            )
          )
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
        val usdcsf    = usdcs.filter(x => !x.from.contains(wtf) && !x.to.contains(wtf))
        val erc1155sf = erc1155s.filter(x => !x.from.contains(wtf) && !x.to.contains(wtf))
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
