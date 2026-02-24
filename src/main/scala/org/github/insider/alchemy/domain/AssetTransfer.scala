package org.github.insider.alchemy.domain

import org.github.insider.alchemy.domain.dto.TokenCategory.{ERC1155, ERC20}
import org.github.insider.alchemy.domain.dto.{TokenCategory, Transfer}

import java.time.LocalDateTime

sealed trait AssetTransfer {
  def blockNum: Long

  def from: String // Make Address value class

  def to: String

  def hash: String

  def blockTimestamp: Option[LocalDateTime]
}

object AssetTransfer {

  /* Converts DTO into domain model */
  def fromTransfer(transfer: Transfer): Option[AssetTransfer] =
    transfer.category match {
      case Some(ERC20) =>
        for {
          blockNumHex <- transfer.blockNum
          from        <- transfer.from
          to          <- transfer.to
          hash        <- transfer.hash
          value       <- transfer.value
        } yield USDCTransfer(
          blockNum       = java.lang.Long.parseLong(blockNumHex.stripPrefix("0x"), 16),
          from           = from,
          to             = to,
          hash           = hash,
          blockTimestamp = transfer.metadata.flatMap(_.blockTimestamp),
          value          = value,
        )

      case Some(ERC1155) =>
        for {
          blockNumHex <- transfer.blockNum
          from        <- transfer.from
          to          <- transfer.to
          hash        <- transfer.hash
          value       <- transfer.erc1155Metadata.headOption.flatMap(_.value)
          tokenId     <- transfer.erc1155Metadata.headOption.flatMap(_.tokenId)
        } yield ERC1155Transfer(
          blockNum       = java.lang.Long.parseLong(blockNumHex.stripPrefix("0x"), 16),
          from           = from,
          to             = to,
          hash           = hash,
          blockTimestamp = transfer.metadata.flatMap(_.blockTimestamp),
          value          = value,
          tokenId        = tokenId
        )
      case _ =>
        for {
          blockNumHex <- transfer.blockNum
          from        <- transfer.from
          to          <- transfer.to
          hash        <- transfer.hash
        } yield UnknownTransfer(
          blockNum       = java.lang.Long.parseLong(blockNumHex.stripPrefix("0x"), 16),
          from           = from,
          to             = to,
          hash           = hash,
          blockTimestamp = transfer.metadata.flatMap(_.blockTimestamp),
          category       = transfer.category,
        )
    }

  final case class ERC1155Transfer(
    blockNum: Long,
    from: String,
    to: String,
    hash: String,
    blockTimestamp: Option[LocalDateTime],
    // ERC1155 transfer metadata
    tokenId: String,
    value: String,
  ) extends AssetTransfer

  final case class USDCTransfer(
    blockNum: Long,
    from: String,
    to: String,
    hash: String,
    blockTimestamp: Option[LocalDateTime],
    // ERC20 transfer metadata
    value: BigDecimal,
  ) extends AssetTransfer

  final case class UnknownTransfer(
    blockNum: Long,
    from: String,
    to: String,
    hash: String,
    blockTimestamp: Option[LocalDateTime],
    // Unknown asset category
    category: Option[TokenCategory],
  ) extends AssetTransfer

}
