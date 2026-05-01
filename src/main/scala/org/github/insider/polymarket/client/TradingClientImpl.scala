package org.github.insider.polymarket.client

import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import org.github.insider.polymarket.configs.MainConfig.PolymarketConfig
import org.github.insider.polymarket.domain.{BuyOrderResult, Position, SellOrderResult, Side, TotalPortfolioValueResult}
import org.http4s.Credentials.Token
import org.http4s.Method.POST
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.circe.CirceSensitiveDataEntityDecoder.circeEntityDecoder
import org.http4s.{AuthScheme, Request, Status, Uri}
import org.http4s.client.{Client, middleware}
import org.http4s.headers.Authorization
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

private class TradingClientImpl[F[_]: Async](
  client: Client[F],
  clobUri: Uri,
  authorization: Authorization,
  userAddress: String,
  logger: Logger[F],
) extends TradingClient[F] {

  override def buy(tokenId: String, money: BigDecimal, maxPrice: Option[BigDecimal]): F[BuyOrderResult] = {

    val request = Request[F](method = POST, uri = clobUri)
      .withHeaders(authorization)
      .withEntity(
        Json.obj(
          "command" -> Json.fromString("trade"),
          "args" -> Json.obj(
            "token_id"   -> Json.fromString(tokenId),
            "price"      -> maxPrice.map(Json.fromBigDecimal).getOrElse(Json.Null),
            "amount"     -> Json.fromBigDecimal(money),
            "side"       -> Side.circeEncoder(Side.Buy),
            "order_type" -> Json.fromString("FAK")
          )
        )
      )

    client.run(request).use {
      case Status.Successful(response) =>
        response
          .as[Json]
          .map(json =>
            for {
              amount     <- json.hcursor.downField("takingAmount").as[BigDecimal]
              totalPrice <- json.hcursor.downField("makingAmount").as[BigDecimal]
            } yield BuyOrderResult(amount, totalPrice)
          )
          .flatMap {
            case Right(result) =>
              logger.info(
                s"${result.amount} tokens $tokenId bought successful with price ${result.totalPrice}"
              ) >> result.pure[F]
            case Left(error) =>
              logger.error(s"Error parsing buy order result from response: ${error.getMessage}") >> Async[F]
                .raiseError(new Throwable(error))
          }
      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while make buy order: $error") >>
            Async[F].raiseError(new Throwable(error))
        }
    }
  }

  override def sell(tokenId: String, shares: BigDecimal, minPrice: Option[BigDecimal]): F[SellOrderResult] = {

    val request = Request[F](method = POST, uri = clobUri)
      .withHeaders(authorization)
      .withEntity(
        Json.obj(
          "command" -> Json.fromString("trade"),
          "args" -> Json.obj(
            "token_id"   -> Json.fromString(tokenId),
            "price"      -> minPrice.map(Json.fromBigDecimal).getOrElse(Json.Null),
            "amount"     -> Json.fromBigDecimal(shares),
            "side"       -> Side.circeEncoder(Side.Sell),
            "order_type" -> Json.fromString("FAK")
          )
        )
      )

    client.run(request).use {
      case Status.Successful(response) =>
        response
          .as[Json]
          .map(json =>
            for {
              amount     <- json.hcursor.downField("makingAmount").as[BigDecimal]
              totalPrice <- json.hcursor.downField("takingAmount").as[BigDecimal]
            } yield SellOrderResult(amount, totalPrice)
          )
          .flatMap {
            case Right(result) =>
              logger.info(
                s"${result.amount} tokens $tokenId sold successful with price ${result.totalPrice}"
              ) >> result.pure[F]
            case Left(error) =>
              logger.error(s"Error parsing sell order result from response: ${error.getMessage}") >> Async[F]
                .raiseError(new Throwable(error))
          }
      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while make sell order: $error") >> Async[F]
            .raiseError(new Throwable(error))
        }
    }
  }

  override def balance(): F[BigDecimal] = {

    val request = Request[F](method = POST, uri = clobUri)
      .withHeaders(authorization)
      .withEntity(Json.obj("command" -> Json.fromString("balance")))

    client.run(request).use {
      case Status.Successful(response) =>
        response
          .as[Json]
          .map(_.hcursor.downField("balance").as[BigDecimal])
          .map(_.map(_ / 1e6))
          .flatMap {
            case Right(balance) =>
              logger.info(s"Balance fetched successfully: $balance") >> balance.pure[F]
            case Left(error) =>
              logger.error(s"Error parsing balance from response: ${error.getMessage}") >> Async[F]
                .raiseError(new Throwable(error))
          }
      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while fetching balance: $error") >>
            Async[F].raiseError(new Throwable(error))
        }
    }
  }

  override def positions(user: Option[String]): F[List[Position]] = {
    val uri: Uri =
      DataApiHost
        .addSegment("positions")
        .withQueryParam("user", user.getOrElse(userAddress))
        .withQueryParam("redeemable", false)

    client.get[List[Position]](uri) {
      case Status.Successful(response) => response.as[List[Position]]
      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while fetching positions: $error") >>
            Async[F].raiseError(new Throwable(error))
        }
    }
  }

  override def portfolioValue(user: Option[String]): F[BigDecimal] = {
    val uri: Uri =
      DataApiHost
        .addSegment("value")
        .withQueryParam("user", user.getOrElse(userAddress))

    client.get[BigDecimal](uri) {
      case Status.Successful(response) =>
        response.as[List[TotalPortfolioValueResult]].map { values =>
          values.map(_.value).sum
        }
      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while fetching total portfolio value: $error") >>
            Async[F].raiseError(new Throwable(error))
        }
    }
  }

  override def buyOrder(tokenId: String, amount: BigDecimal, price: BigDecimal): F[Unit] = {

    val request = Request[F](method = POST, uri = clobUri)
      .withHeaders(authorization)
      .withEntity(
        Json.obj(
          "command" -> Json.fromString("trade"),
          "args" -> Json.obj(
            "token_id"   -> Json.fromString(tokenId),
            "price"      -> Json.fromBigDecimal(price),
            "amount"     -> Json.fromBigDecimal(amount),
            "side"       -> Side.circeEncoder(Side.Buy),
            "order_type" -> Json.fromString("GTC")
          )
        )
      )

    client.run(request).use {
      case Status.Successful(response) =>
        ()
        response
          .as[Json]
          .map(json => {
            val resp = json.hcursor.downField("success").as[Boolean].toOption
            if (resp.contains(true))
              logger.info(s"Buy order placed successfully for token $tokenId with amount $amount and price $price")
            else {
              val error = json.findAllByKey("errorMsg").headOption.flatMap(_.asString).getOrElse("Unknown error")
              logger
                .error(s"Unsuccessful response received while placing buy order: $error") >> Async[F]
                .raiseError(new Throwable(error))
            }
          })

      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while placing buy order: $error") >>
            Async[F].raiseError(new Throwable(error))
        }
    }
  }

  override def sellOrder(tokenId: String, shares: BigDecimal, price: BigDecimal): F[Unit] = {

    val request = Request[F](method = POST, uri = clobUri)
      .withHeaders(authorization)
      .withEntity(
        Json.obj(
          "command" -> Json.fromString("trade"),
          "args" -> Json.obj(
            "token_id"   -> Json.fromString(tokenId),
            "price"      -> Json.fromBigDecimal(price),
            "amount"     -> Json.fromBigDecimal(shares),
            "side"       -> Side.circeEncoder(Side.Sell),
            "order_type" -> Json.fromString("GTC")
          )
        )
      )

    client.run(request).use {
      case Status.Successful(response) =>
        ()
        response
          .as[Json]
          .map(json => {
            val resp = json.hcursor.downField("success").as[Boolean].toOption
            if (resp.contains(true))
              logger.info(s"Buy order placed successfully for token $tokenId with shares $shares and price $price")
            else {
              val error = json.findAllByKey("errorMsg").headOption.flatMap(_.asString).getOrElse("Unknown error")
              logger
                .error(s"Unsuccessful response received while placing sell order: $error") >> Async[F]
                .raiseError(new Throwable(error))
            }
          })

      case other =>
        other.as[Json].flatMap { json =>
          val error = json.findAllByKey("error").headOption.flatMap(_.asString).getOrElse("Unknown error")
          logger.error(s"Unsuccessful response received while placing sell order: $error") >>
            Async[F].raiseError(new Throwable(error))
        }
    }
  }
}

object TradingClientImpl {
  def of[F[_]: Async](client: Client[F], config: PolymarketConfig): F[TradingClient[F]] = {
    val clientWithLogging = middleware.Logger[F](logBody = false, logHeaders = false)(client)
    val clobUri           = Uri.unsafeFromString(config.clobAddress)
    val authorization     = Authorization(Token(AuthScheme.Bearer, config.bearer))

    Slf4jLogger
      .create[F]
      .map(logger => new TradingClientImpl[F](clientWithLogging, clobUri, authorization, config.userAddress, logger))
  }
}
