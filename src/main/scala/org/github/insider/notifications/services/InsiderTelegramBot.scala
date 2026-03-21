package org.github.insider.notifications.services

import cats.effect.Async
import cats.syntax.all._
import com.bot4s.telegram.cats.Polling
import com.bot4s.telegram.methods._
import com.bot4s.telegram.models._
import com.bot4s.telegram.cats.TelegramBot
import org.github.insider.leaderboard.TradeNotification
import sttp.client4.Backend

class InsiderTelegramBot[F[_]: Async](token: String, chatId: ChatId, backend: Backend[F])
    extends TelegramBot[F](token, backend)
    with Polling[F] {

  def sendNotifications(notifications: List[TradeNotification]): F[Unit] =
    notifications.traverse_ { notification =>
      request(SendMessage(chatId, toMessage(notification)))
    }

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
       |Wallet address: ${notification.trade.makerAddress}
       |Operation side: ${notification.trade.side}
       |Tokens amount: ${notification.trade.amount / 1_000_000}
       |Total price: ${notification.trade.totalPrice}
       |Single token price: ${notification.trade.singleTokenPrice}
       |Block Timestamp: ${notification.trade.blockTimestamp.getOrElse("???")}
       |Token ID: ${notification.trade.tokenId}
       |Event link: ${notification.event.slug.map(slug => s"https://polymarket.com/event/$slug").getOrElse("")}
       |Market question: ${notification.event.markets.flatMap(_.headOption.map(_.question)).getOrElse("???")}
       |$leaderboard
       |""".stripMargin
  }
}
