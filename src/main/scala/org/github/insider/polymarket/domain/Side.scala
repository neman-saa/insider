package org.github.insider.polymarket.domain

import doobie.Meta
import io.circe.{Decoder, Encoder}

sealed trait Side {
  def sign: Int
}

object Side {
  case object Buy extends Side {
    override def sign: Int = 1
  }
  case object Sell extends Side {
    override def sign: Int = -1
  }

  implicit val circeDecoder: Decoder[Side] =
    Decoder[String].map {
      case "BUY"  => Side.Buy
      case "SELL" => Side.Sell
    }

  implicit val circeEncoder: Encoder[Side] =
    Encoder[String].contramap {
      case Side.Buy  => "BUY"
      case Side.Sell => "SELL"
    }

  implicit val sideMeta: Meta[Side] = Meta[String].timap[Side] {
    case "BUY"  => Buy
    case "SELL" => Sell
  } {
    case Buy  => "BUY"
    case Sell => "SELL"
  }
}
