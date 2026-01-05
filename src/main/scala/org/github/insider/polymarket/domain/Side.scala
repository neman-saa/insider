package org.github.insider.polymarket.domain

import doobie.Meta
import io.circe.Decoder

sealed trait Side

object Side {
  final case object Buy  extends Side
  final case object Sell extends Side

  implicit val circeDecoder: Decoder[Side] =
    Decoder[String].map {
      case "BUY"  => Side.Buy
      case "SELL" => Side.Sell
    }

  implicit val sideMeta: Meta[Side] = Meta[String].timap[Side]{
    case "BUY" => Buy
    case "SELL" => Sell
  }{
    case Buy => "BUY"
    case Sell => "SELL"
  }
}
