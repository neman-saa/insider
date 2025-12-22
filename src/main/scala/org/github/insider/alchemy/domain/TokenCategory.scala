package org.github.insider.alchemy.domain

import io.circe.{Decoder, Encoder}

sealed trait TokenCategory

object TokenCategory {
  final case object ERC20               extends TokenCategory
  final case object ERC1155             extends TokenCategory
  final case class Other(value: String) extends TokenCategory

  implicit val encoder: Encoder[TokenCategory] =
    Encoder.encodeString.contramap {
      case ERC20        => "erc20"
      case ERC1155      => "erc1155"
      case Other(value) => value
    }
  implicit val decoder: Decoder[TokenCategory] =
    Decoder.decodeString.map {
      case "erc20"   => ERC20
      case "erc1155" => ERC1155
      case other     => Other(other)
    }
}
