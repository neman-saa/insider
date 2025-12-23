package org.github.insider.alchemy.domain

import io.circe.Codec
import io.circe.generic.semiauto.deriveCodec
import org.github.insider.alchemy.domain.TokenCategory.{ERC1155, ERC20}
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
) {

  val wtf        = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296".toLowerCase
  val negRiskCtf = "0xC5d563A36AE78145C45a50134d48A1215220f80a".toLowerCase

  def matchTransfer: Option[Either[UsdcTransfer, Erc1155Transfer]] = this match {
    case Transfer(_, _, Some(from), Some(to), _, _, _, _, _, _, _, _)
        if from.toLowerCase == wtf || to.toLowerCase == wtf =>
      None
    case Transfer(Some(ERC20), _, Some(from), Some(to), Some(value), _, _, _, Some("USDCE"), _, Some(hash), _) =>
      Some(Left(UsdcTransfer(from, to, value, hash)))
    case Transfer(
          Some(ERC1155),
          _,
          Some(from),
          Some(to),
          _, _,
          List(ERC1155TransferMetadata(Some(tokenId), Some(value))),
          _, _, _,
          Some(hash),
          _) =>
      Some(Right(Erc1155Transfer(from, to, BigDecimal(value), tokenId, hash)))
    case _ => None
  }
}

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
