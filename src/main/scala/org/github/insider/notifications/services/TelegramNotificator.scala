package org.github.insider.notifications.services

import cats.effect.Async
import canoe.api._
import canoe.syntax._
import fs2.concurrent.Topic
import org.github.insider.polymarket.domain.Trade

class TelegramNotificator[F[_]: Async: TelegramClient](topic: Topic[F, Trade]) {

  def app: F[Unit] = Bot
    .polling[F]
    .follow(notifications)
    .compile
    .drain

  private def toMessage(trade: Trade): String =
    s"""
      |User with address: ${trade.makerAddress}
      |bought ${trade.amount} tokens for
      |""".stripMargin

  private def notifications: Scenario[F, Unit] = for {
    chat <- Scenario.expect(command("start").chat)
    _    <- Scenario.eval(chat.send("Started, you will get message when trade appear"))
    fiber <- Scenario.eval(topic.subscribe(10).evalMap(trade => chat.send()))
    _    <- Scenario.done
  } yield ()

}
