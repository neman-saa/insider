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

class TelegramNotificator[F[_]: Async: TelegramClient](topic: Topic[F, Trade]) {

  def create: F[Unit] = Bot
    .polling[F]
    .follow(notifications)
    .compile
    .drain

  private def toMessage(trade: Trade): String =
    s"""
      |User with address: ${trade.makerAddress}
      |bought ${trade.amount} tokens for ${trade.totalPrice}
      |SIDE = ${trade.side}
      |timestamp = ${trade.blockTimestamp.getOrElse("???")}
      |block number = ${trade.blockNum}
      |""".stripMargin

  private def notifications: Scenario[F, Unit] =
    for {
      chat <- Scenario.expect(command("start").chat)
      _    <- Scenario.eval(chat.send("Started, you will get message when trade appear"))
      fiber <-
        Scenario.eval(
          topic
            .subscribe(10)
            .evalMap(trade => chat.send(toMessage(trade)))
            .compile
            .drain
            .start
        )
      _ <- Scenario.expect(command("stop"))
      _ <- Scenario.eval(fiber.cancel)
      _ <- Scenario.done
    } yield ()

}

object TelegramNotificator {
  def of[F[_]: Async: TelegramClient: Logger](topic: Topic[F, Trade]): F[TelegramNotificator[F]] =
    Slf4jLogger.create[F].map(_ => new TelegramNotificator[F](topic))
}
