package org.github.insider.alchemy.domain

import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.domain.dto.{TokenCategory, Transfer}
import org.github.insider.polymarket.domain.Trade

sealed trait AssetTransfer {
  def blockNum: Option[String]

  def from: Option[String] // Make Address value class

  def to: Option[String]

  def hash: Option[String]

  def blockTimestamp: Option[String]
}

object AssetTransfer {

  /* Converts DTO into domain model */
  def fromTransfer(transfer: Transfer): List[AssetTransfer] =
    transfer.category match {
      case Some(ERC20) =>
        List(
          USDCTransfer(
            blockNum       = transfer.blockNum,
            from           = transfer.from,
            to             = transfer.to,
            hash           = transfer.hash,
            blockTimestamp = transfer.metadata.flatMap(_.blockTimestamp),
            value          = transfer.value,
          )
        )
      case Some(ERC1155) =>
        transfer.erc1155Metadata.map { erc1155TransferMetadata =>
          ERC1155Transfer(
            blockNum       = transfer.blockNum,
            from           = transfer.from,
            to             = transfer.to,
            hash           = transfer.hash,
            blockTimestamp = transfer.metadata.flatMap(_.blockTimestamp),
            tokenId        = erc1155TransferMetadata.tokenId,
            value          = erc1155TransferMetadata.value,
          )
        }
      case _ =>
        List(
          UnknownTransfer(
            blockNum       = transfer.blockNum,
            from           = transfer.from,
            to             = transfer.to,
            hash           = transfer.hash,
            blockTimestamp = transfer.metadata.flatMap(_.blockTimestamp),
            category       = transfer.category,
          )
        )
    }

  private final case class ERC1155Transfer(
    blockNum: Option[String],
    from: Option[String],
    to: Option[String],
    hash: Option[String],
    blockTimestamp: Option[String],
    // ERC1155 transfer metadata
    tokenId: Option[String],
    value: Option[String],
  ) extends AssetTransfer

  private final case class USDCTransfer(
    blockNum: Option[String],
    from: Option[String],
    to: Option[String],
    hash: Option[String],
    blockTimestamp: Option[String],
    // ERC20 transfer metadata
    value: Option[BigDecimal],
  ) extends AssetTransfer

  private final case class UnknownTransfer(
    blockNum: Option[String],
    from: Option[String],
    to: Option[String],
    hash: Option[String],
    blockTimestamp: Option[String],
    // Unknown asset category
    category: Option[TokenCategory],
  ) extends AssetTransfer

  def getTradesFromBlock(transfers: List[AssetTransfer]): List[Trade] = {

    def matchTransfers(usdcs: List[USDCTransfer], erc1155s: List[ERC1155Transfer]): List[Trade] = ???

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
