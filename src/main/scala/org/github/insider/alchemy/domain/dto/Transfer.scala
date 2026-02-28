package org.github.insider.alchemy.domain.dto

import io.circe.{Codec, Decoder}
import io.circe.generic.semiauto.deriveCodec
import org.github.insider.alchemy.domain.dto.Transfer.{ERC1155TransferMetadata, TransferMetadata}

import java.time.LocalDateTime

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
    blockTimestamp: Option[LocalDateTime],
  )

  object TransferMetadata {
    implicit val codec: Codec[TransferMetadata] = deriveCodec

    implicit val localDateTimeDecoder: Decoder[LocalDateTime] =
      Decoder[String].map(_.init).map(LocalDateTime.parse)
  }
}
