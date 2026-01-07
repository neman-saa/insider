package org.github.insider.alchemy.domain

import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.domain.dto.{TokenCategory, Transfer}

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

}
