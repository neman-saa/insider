package org.github.insider.polymarket.domain

import cats.syntax.all._
import io.circe._

import java.time.Instant

final case class Market(
  id: String,
  question: String,
  conditionId: String,
  volume: Option[Volume],
  tokens: List[Token],
  startDate: Option[Instant],
  endDate: Option[Instant]
)

object Market {
  implicit val circeDecoder: Decoder[Market] =
    Decoder.instance[Market] { c =>
      for {
        id          <- c.downField("id").as[String]
        question    <- c.downField("question").as[String]
        conditionId <- c.downField("conditionId").as[String]
        startDate   <- c.downField("startDate").as[Option[Instant]]
        endDate     <- c.downField("endDate").as[Option[Instant]]
        volume      <- c.downField("volume").as[Option[Volume]]

        stringOutcomes <- c.downField("outcomes").as[String]
        outcomesJson <- parser.parse(stringOutcomes).leftMap(parseFailure => DecodingFailure(parseFailure.message, Nil))
        outcomes     <- outcomesJson.as[List[Option[String]]]

        stringTokenIds <- c.downField("clobTokenIds").as[Option[String]]
        tokenIdsJson <- stringTokenIds
          .map(s => parser.parse(s).leftMap(parseFailure => DecodingFailure(parseFailure.message, Nil)))
          .getOrElse(Right(Json.arr(Vector.fill(outcomes.length)(Json.Null): _*)))
        tokenIds <- tokenIdsJson.as[List[Option[String]]]

        stringOutcomesPrices <- c.downField("outcomePrices").as[Option[String]]
        outcomesPricesJson <- stringOutcomesPrices
          .map(s => parser.parse(s).leftMap(parseFailure => DecodingFailure(parseFailure.message, Nil)))
          .getOrElse(Right(Json.arr(Vector.fill(outcomes.length)(Json.Null): _*)))
        outcomesPrices <- outcomesPricesJson.as[List[Option[Volume]]]

        tokens = outcomes
          .zipAll(tokenIds, None, None)
          .zipAll(outcomesPrices, (None, None), None)
          .map {
            case ((outcome, tokenId), price) => Token(outcome, tokenId, price)
          }
      } yield Market(id, question, conditionId, volume, tokens, startDate, endDate)
    }
}
