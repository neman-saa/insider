package org.github.insider.polymarket.domain

import cats.syntax.all._
import io.circe._

final case class Market(
  id: String,
  question: String,
  conditionId: String,
  volume: Volume,
  tokens: List[Token],
)

object Market {
  implicit val circeDecoder: Decoder[Market] =
    Decoder.instance[Market] { c =>
      for {
        id          <- c.downField("id").as[String]
        question    <- c.downField("question").as[String]
        conditionId <- c.downField("conditionId").as[String]
        volume      <- c.downField("volume").as[Volume]

        stringOutcomes <- c.downField("outcomes").as[String]
        outcomesJson <- parser.parse(stringOutcomes).leftMap(parseFailure => DecodingFailure(parseFailure.message, Nil))
        outcomes     <- outcomesJson.as[List[Outcome]]

        stringTokenIds <- c.downField("clobTokenIds").as[String]
        tokenIdsJson <- parser.parse(stringTokenIds).leftMap(parseFailure => DecodingFailure(parseFailure.message, Nil))
        tokenIds     <- tokenIdsJson.as[List[String]]

        tokens = (outcomes zip tokenIds).map { case (outcome, tokenId) => Token(outcome, tokenId) }
      } yield Market(id, question, conditionId, volume, tokens)
    }
}
