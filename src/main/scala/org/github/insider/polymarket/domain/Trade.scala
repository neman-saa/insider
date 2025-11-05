package org.github.insider.polymarket.domain

import io.circe.Decoder

case class Trade(
  wallet: String,
  token: Token,
  side: Side,
  size: BigDecimal,
  price: BigDecimal,
  timestamp: Long
)

object Trade {
  implicit val circeDecoder: Decoder[Trade] =
    Decoder.instance[Trade] { c =>
      for {
        wallet    <- c.downField("proxyWallet").as[String]
        tokenId   <- c.downField("asset").as[String]
        outcome   <- c.downField("outcome").as[Outcome]
        timestamp <- c.downField("timestamp").as[Long]
        size      <- c.downField("size").as[BigDecimal]
        price     <- c.downField("price").as[BigDecimal]
        side      <- c.downField("side").as[Side]
      } yield Trade(wallet, Token(outcome, tokenId), side, size, price, timestamp)
    }
}
