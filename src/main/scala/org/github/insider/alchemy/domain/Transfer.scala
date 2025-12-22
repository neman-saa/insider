package org.github.insider.alchemy.domain

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.github.insider.alchemy.domain.Transfer.{ERC1155TransferMetadata, TransferMetadata}

final case class Transfer(
  category: Option[TokenCategory],
  blockNum: Option[String],
  from: Option[String],
  to: Option[String],
  value: Option[BigDecimal],
  erc721TokenId: Option[String],
  erc1155Metadata: List[ERC1155TransferMetadata],
  tokenId: Option[String],
  asset: Option[String],
  uniqueId: Option[String],
  hash: Option[String],
  metadata: Option[TransferMetadata],
)

object Transfer {
  implicit val codec: Codec[Transfer] = deriveCodec

  final case class ERC1155TransferMetadata(
    tokenId: Option[String],
    value: Option[String],
  )

  object ERC1155TransferMetadata {
    implicit val codec: Codec[ERC1155TransferMetadata] = deriveCodec
  }

  final case class TransferMetadata(
    blockTimestamp: Option[String],
  )

  object TransferMetadata {
    implicit val codec: Codec[TransferMetadata] = deriveCodec
  }
}
