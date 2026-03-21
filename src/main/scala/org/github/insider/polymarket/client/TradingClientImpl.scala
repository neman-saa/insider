package org.github.insider.polymarket.client

import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import org.github.insider.polymarket.configs.MainConfig.PolymarketConfig
import org.github.insider.polymarket.domain.{BuyOrderResult, Position, SellOrderResult, Side, Tag}
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.circe.CirceSensitiveDataEntityDecoder.circeEntityDecoder
import org.http4s.{Request, Status, Uri}
import org.http4s.client.{Client, middleware}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

private class TradingClientImpl[F[_]: Async](
  client: Client[F],
  clobUri: Uri,
  user: String,
  logger: Logger[F],
) extends TradingClient[F] {

  override def buy(tokenId: String, money: BigDecimal, maxPrice: Option[BigDecimal]): F[Option[BuyOrderResult]] = {

    val request = Request[F](method = POST, uri = clobUri).withEntity(
      Json.obj(
        "command" -> Json.fromString("trade"),
        "args" -> Json.obj(
          "token_id" -> Json.fromString(tokenId),
          "price"    -> maxPrice.map(Json.fromBigDecimal).getOrElse(Json.Null),
          "amount"   -> Json.fromBigDecimal(money),
          "side"     -> Side.circeEncoder(Side.Buy)
        )
      )
    )

    client.run(request).use {
      case Status.Successful(response) =>
        response
          .as[Json]
          .map(json =>
            for {
              amount     <- json.hcursor.downField("takingAmount").as[BigDecimal].toOption
              totalPrice <- json.hcursor.downField("makingAmount").as[BigDecimal].toOption
            } yield BuyOrderResult(amount, totalPrice)
          )
      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while make order: $error") >>
            none[BuyOrderResult].pure[F]
        }
    }
  }

  override def sell(tokenId: String, entity: BigDecimal, minPrice: Option[BigDecimal]): F[Option[SellOrderResult]] = {

    val request = Request[F](method = POST, uri = clobUri).withEntity(
      Json.obj(
        "command" -> Json.fromString("trade"),
        "args" -> Json.obj(
          "token_id" -> Json.fromString(tokenId),
          "price"    -> minPrice.map(Json.fromBigDecimal).getOrElse(Json.Null),
          "amount"   -> Json.fromBigDecimal(entity),
          "side"     -> Side.circeEncoder(Side.Sell)
        )
      )
    )

    client.run(request).use {
      case Status.Successful(response) =>
        response
          .as[Json]
          .map(json =>
            for {
              amount     <- json.hcursor.downField("makingAmount").as[BigDecimal].toOption
              totalPrice <- json.hcursor.downField("takingAmount").as[BigDecimal].toOption
            } yield SellOrderResult(amount, totalPrice)
          )
      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while make order: $error") >>
            none[SellOrderResult].pure[F]
        }
    }
  }

  override def balance(): F[Option[BigDecimal]] = {

    val request = Request[F](method = POST, uri = clobUri).withEntity(Json.obj("command" -> Json.fromString("balance")))

    client.run(request).use {
      case Status.Successful(response) =>
        response
          .as[Json]
          .map(_.hcursor.downField("balance").as[BigDecimal].toOption)
          .map(_.map(_ / 1e6))
      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while make order: $error") >>
            none[BigDecimal].pure[F]
        }
    }
  }

  override def positions(): F[List[Position]] = {
    val uri: Uri =
      DataApiHost
        .addSegment("positions")
        .withQueryParam("user", user)

    client.get[List[Position]](uri) {
      case Status.Successful(response) => response.as[List[Position]]
      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while fetching positions: $error") >>
            List[Position]().pure[F]
        }
    }
  }
}

object TradingClientImpl {
  def of[F[_]: Async](client: Client[F], config: PolymarketConfig): F[TradingClient[F]] = {
    val clientWithLogging = middleware.Logger[F](logBody = false, logHeaders = false)(client)
    val clobUri           = Uri.unsafeFromString(config.clobAddress)

    Slf4jLogger.create[F].map(logger => new TradingClientImpl[F](clientWithLogging, clobUri, config.user, logger))
  }
}
