package org.github.insider.notifications.services

import cats.effect.Async
import canoe.api._
import canoe.syntax._
import fs2.concurrent.Topic
import org.github.insider.polymarket.domain.Trade
import cats.syntax.all._
import cats.effect.implicits._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object TelegramNotificator {

  def create[F[_]: Async: TelegramClient](importantTrades: Topic[F, Trade]): F[Unit] =
    Bot
      .polling[F]
      .follow(notifications[F](importantTrades))
      .compile
      .drain

  private def toMessage(trade: Trade): String =
    s"""
      |User with address: ${trade.makerAddress}
      |bought ${trade.amount} tokens for ${trade.totalPrice}
      |SIDE = ${trade.side}
      |timestamp = ${trade.blockTimestamp.getOrElse("???")}
      |block number = ${trade.blockNum}
      |token id = ${trade.tokenId}
      |""".stripMargin

  private def notifications[F[_]: Async: TelegramClient](topic: Topic[F, Trade]): Scenario[F, Unit] =
    for {
      chat <- Scenario.expect(command("start").chat)
      _    <- Scenario.eval(chat.send("Started, you will get message when trade appear"))
      fiber <-
        Scenario.eval(
          topic
            .subscribeUnbounded
            .evalMap(trade => chat.send(toMessage(trade)))
            .compile
            .drain
            .start
        )
      _ <- Scenario.expect(command("stop"))
      _ <- Scenario.eval(fiber.cancel)
      _ <- Scenario.eval(chat.send("Stopped"))
      _ <- Scenario.done
    } yield ()

}
