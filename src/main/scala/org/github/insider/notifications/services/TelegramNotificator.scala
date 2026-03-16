package org.github.insider.notifications.services

import cats.effect.Async
import canoe.api._
import canoe.syntax._
import fs2.concurrent.Topic
import org.github.insider.polymarket.domain.Trade
import cats.syntax.all._
import cats.effect.implicits._
import org.github.insider.leaderboard.TradeNotification
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

object TelegramNotificator {

  def create[F[_]: Async: TelegramClient](importantTrades: Topic[F, TradeNotification]): F[Unit] =
    Bot
      .polling[F]
      .follow(notifications[F](importantTrades))
      .compile
      .drain

  private def notifications[F[_]: Async: TelegramClient](topic: Topic[F, TradeNotification]): Scenario[F, Unit] =
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

  private def toMessage(notification: TradeNotification): String = {
    val leaderboard =
      notification
        .leaderboardEntries
        .map {
          case (name, entry) =>
            s"""
           |Leaderboard name: ${name.value}
           |Leaderboard stat: ${entry.prettyPrint}
           |""".stripMargin
        }
        .mkString("\n")

    s"""
       |User address: ${notification.trade.makerAddress}
       |Operation side: ${notification.trade.side}
       |Tokens amount: ${notification.trade.amount / 1_000_000}
       |Total price: ${notification.trade.totalPrice}
       |Single token price: ${notification.trade.singleTokenPrice}
       |Block Timestamp: ${notification.trade.blockTimestamp.getOrElse("???")}
       |Token ID: ${notification.trade.tokenId}
       |Event link: ${notification.event.slug.map(slug => s"https://polymarket.com/event/$slug").getOrElse("")}
       |Event slug: ${notification.event.slug.getOrElse("")}
       |Market question: ${notification.event.markets.flatMap(_.headOption.map(_.question)).getOrElse("???")}
       |$leaderboard
       |""".stripMargin
  }

}
