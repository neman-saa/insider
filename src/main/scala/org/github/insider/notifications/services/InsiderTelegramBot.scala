package org.github.insider.notifications.services

import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.bot4s.telegram.cats.Polling
import com.bot4s.telegram.methods._
import com.bot4s.telegram.models._
import com.bot4s.telegram.cats.TelegramBot
import org.github.insider.leaderboard.TradeNotification
import sttp.client4.Backend

class InsiderTelegramBot[F[_]: Async](token: String, chatId: ChatId, backend: Backend[F])(
  followTokens: Ref[F, Set[String]]
) extends TelegramBot[F](token, backend)
    with Polling[F] {

  def sendNotifications(notifications: List[TradeNotification]): F[Unit] =
    notifications
      .traverse_ { notification =>
        request(SendMessage(chatId, toMessage(notification)))
      }
      .handleErrorWith(_ => Async[F].unit)

  override def receiveChannelPost(message: Message): F[Unit] = {
    if (message.chat.chatId != chatId) Async[F].unit

    val action = message.text match {
      case Some(msg) if msg.startsWith("/follow") =>
        val maybeTokenToFollow = msg.split(" ").toList.get(1)

        maybeTokenToFollow match {
          case Some(token) =>
            followTokens.getAndUpdate(tokens => tokens.incl(token)) >>
              request(SendMessage(chatId, "Token was successfully included to follow list ✅")).void
          case None =>
            request(SendMessage(chatId, "Token for follow is not provided ❌")).void
        }
      case Some(msg) if msg.startsWith("/unfollow") =>
        val maybeTokenToUnfollow = msg.split(" ").toList.get(1)

        maybeTokenToUnfollow match {
          case Some(token) =>
            followTokens.getAndUpdate(tokens => tokens.excl(token)) >>
              request(SendMessage(chatId, "Token was successfully excluded to follow list ✅")).void
          case None =>
            request(SendMessage(chatId, "Token for unfollow is not provided ❌")).void
        }
      case _ => Async[F].unit
    }

    action.handleErrorWith(_ => Async[F].unit)
  }

  private def toMessage(notification: TradeNotification): String = {
    val tokenOutcome: Option[String] =
      for {
        markets <- notification.event.markets
        market  <- markets.headOption
        token   <- market.tokens.find(_.id.contains(notification.trade.tokenId))
        outcome <- token.outcome
      } yield outcome

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
       |Block Timestamp: ${notification.trade.blockTimestamp.getOrElse("Unknown")}
       |Token ID: ${notification.trade.tokenId}
       |Event link: ${notification.event.slug.map(slug => s"https://polymarket.com/event/$slug").getOrElse("Unknown")}
       |Market question: ${notification.event.markets.flatMap(_.headOption.map(_.question)).getOrElse("Unknown")}
       |Outcome: ${tokenOutcome.getOrElse("Unknown")}
       |$leaderboard
       |""".stripMargin
  }
}
