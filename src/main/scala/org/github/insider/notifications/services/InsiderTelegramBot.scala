package org.github.insider.notifications.services

import cats.effect.{Async, Ref}
import cats.syntax.all._
import com.bot4s.telegram.cats.Polling
import com.bot4s.telegram.methods._
import com.bot4s.telegram.models._
import com.bot4s.telegram.cats.TelegramBot
import org.github.insider.leaderboard.TradeNotification
import org.github.insider.polymarket.client.TradingClient
import org.github.insider.polymarket.domain.Position
import sttp.client4.Backend
import org.github.insider.polymarket.domain.Side

class InsiderTelegramBot[F[_]: Async](token: String, chatId: ChatId, backend: Backend[F])(
  followTokens: Ref[F, Map[String, Set[String]]], // token id to set of usernames
  tradingClient: TradingClient[F],
) extends TelegramBot[F](token, backend)
    with Polling[F] {

  def sendAlive: F[Unit] =
    request(SendMessage(chatId, "I AM ALIVE")).void

  def sendNotifications(notifications: List[TradeNotification]): F[Unit] =
    notifications
      .traverse_ { notification =>
        request(SendMessage(chatId, tradeNotificationToMessage(notification)))
      }
      .handleErrorWith(_ => Async[F].unit)

  def sendTradeInfo(message: String): F[Unit] = {
    request(SendMessage(chatId, message))
  }.void.handleErrorWith(_ => Async[F].unit)

  override def receiveMessage(message: Message): F[Unit] = {
    if (message.chat.chatId != chatId) Async[F].unit

    val action = message.text match {
      case Some(msg) if msg.startsWith("/portfolio-value") =>
        tradingClient.portfolioValue().flatMap { portfolioValue =>
          request(SendMessage(chatId, portfolioValueToMessage(portfolioValue))).void
        }

      case Some(msg) if msg.startsWith("/balance") =>
        tradingClient.balance().flatMap { balance =>
          request(SendMessage(chatId, balanceToMessage(balance))).void
        }

      case Some(msg) if msg.startsWith("/positions") =>
        tradingClient.positions().flatMap { positions =>
          request(SendMessage(chatId, positionsToMessage(positions))).void
        }

      case Some(msg) if msg.startsWith("/follow-list") =>
        val maybeUsername = message.from.flatMap(_.username)

        maybeUsername match {
          case Some(username) =>
            followTokens.get.flatMap { tokens =>
              val followedTokens = tokens.collect {
                case (token, usernames) if usernames.contains(username) => token
              }.toList

              request(SendMessage(chatId, followListToMessage(username, followedTokens))).void
            }
          case None =>
            request(SendMessage(chatId, "Can't extract your Telegram username ❌")).void
        }

      case Some(msg) if msg.startsWith("/follow") =>
        val maybeUsername      = message.from.flatMap(_.username)
        val maybeTokenToFollow = msg.split(" ").toList.get(1)

        maybeUsername match {
          case Some(username) =>
            maybeTokenToFollow match {
              case Some(token) =>
                followTokens.getAndUpdate { tokens =>
                  val existingFollowers   = tokens.getOrElse(token, Set.empty)
                  val updatedFollowersSet = existingFollowers.incl(username)

                  tokens.updated(token, updatedFollowersSet)
                } >> request(
                  SendMessage(chatId, "Token was successfully included to follow list, you will be pinged ✅")
                ).void
              case None =>
                request(SendMessage(chatId, "Token for follow is not provided ❌")).void
            }
          case None =>
            request(SendMessage(chatId, "Can't extract your Telegram username ❌")).void
        }

      case Some(msg) if msg.startsWith("/unfollow") =>
        val maybeUsername        = message.from.flatMap(_.username)
        val maybeTokenToUnfollow = msg.split(" ").toList.get(1)

        maybeUsername match {
          case Some(username) =>
            maybeTokenToUnfollow match {
              case Some(token) =>
                followTokens.getAndUpdate { tokens =>
                  val existingFollowers   = tokens.getOrElse(token, Set.empty)
                  val updatedFollowersSet = existingFollowers.excl(username)

                  if (updatedFollowersSet.isEmpty) tokens.removed(token)
                  else tokens.updated(token, updatedFollowersSet)
                } >> request(SendMessage(chatId, "Token was successfully excluded from follow list ✅")).void
              case None =>
                request(SendMessage(chatId, "Token for unfollow is not provided ❌")).void
            }
          case None =>
            request(SendMessage(chatId, "Can't extract your Telegram username ❌")).void
        }
      case _ => Async[F].unit
    }

    action.handleErrorWith(_ => Async[F].unit)
  }

  private def tradeNotificationToMessage(notification: TradeNotification): String = {
    val followersStr = {
      if (notification.followers.isEmpty) ""
      else {
        val followersListStr = notification.followers.map(followerUsername => s"@$followerUsername").mkString(" ")

        s"""
           |Followers $followersListStr
           |""".stripMargin
      }
    }

    val tokenOutcome: Option[String] =
      for {
        event   <- notification.event
        markets <- event.markets
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

    // format: off
    s"""
       |Wallet address: ${notification.trade.makerAddress}
       |Operation side: ${notification.trade.side}
       |Tokens amount: ${notification.trade.amount / 1_000_000}
       |Total price: ${notification.trade.totalPrice}
       |Single token price: ${notification.trade.singleTokenPrice}
       |Block Timestamp: ${notification.trade.blockTimestamp.getOrElse("Unknown")}
       |Token ID: ${notification.trade.tokenId}
       |Event link: ${notification.event.flatMap(_.slug).map(slug => s"https://polymarket.com/event/$slug").getOrElse("Unknown")}
       |Market question: ${notification.event.flatMap(_.markets).flatMap(_.headOption.map(_.question)).getOrElse("Unknown")}
       |Outcome: ${tokenOutcome.getOrElse("Unknown")}
       |$leaderboard
       |$followersStr
       |""".stripMargin
    // format: on
  }

  private def followListToMessage(username: String, tokens: List[String]): String = {
    val tokensListStr = tokens.map(token => "- " + token).mkString("\n")

    s"""
       |Tokens followed by @$username (${tokens.size})
       |$tokensListStr
       |""".stripMargin
  }

  private def balanceToMessage(balance: BigDecimal): String =
    s"Current wallet balance: $balance"

  private def portfolioValueToMessage(portfolioValue: BigDecimal): String =
    s"Current portfolio value: $portfolioValue"

  private def positionsToMessage(positions: List[Position]): String = {
    val positionsListStr = positions.map(position => s"- asset ${position.asset} size ${position.size}").mkString("\n")

    s"""
       |Current wallet positions:
       |$positionsListStr
       |""".stripMargin
  }
}
