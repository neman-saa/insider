package org.github.insider.polymarket

import cats.Parallel
import cats.effect.Async
import cats.effect.implicits.effectResourceOps
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.evolution.scache.{Cache, ExpiringCache}
import org.github.insider.polymarket.client.EventsClient
import org.github.insider.polymarket.domain.Event
import org.github.insider.realtime.tokens.{TokenId, TokenMetaInfo}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.{DurationInt, FiniteDuration}

class EventsCached[F[_]: Async](logger: Logger[F], cache: Cache[F, TokenId, Event], eventsClient: EventsClient[F]) {

  def getTokensMetaInfo(tokenIds: Set[TokenId]): F[Map[TokenId, TokenMetaInfo]] = {
    tokenIds
      .toList
      .traverse { tokenId =>
        getEvent(tokenId).map { maybeEvent =>
          val maybeMarket = maybeEvent.flatMap(event =>
            event.markets.flatMap { markets =>
              markets.find { market =>
                market.tokens.flatMap(_.id).contains(tokenId)
              }
            }
          )

          val maybeTokenMetaInfo =
            for {
              market          <- maybeMarket
              oppositeTokenId <- market.tokens.flatMap(_.id).find(_ != tokenId)
              resolveDate     <- market.endDate // Is this correct?
            } yield TokenMetaInfo(tokenId, oppositeTokenId, resolveDate)

          maybeTokenMetaInfo.map(tokenMetaInfo => tokenId -> tokenMetaInfo)
        }
      }
      .map(_.flatten.toMap)
  }

  def getEvent(tokenId: TokenId): F[Option[Event]] =
    cache.getOrUpdateOpt(tokenId) {
      eventsClient.getEventsByTokens(List(tokenId)).map { events =>
        events.headOption
      }
    }
}

object EventsCached {
  def of[F[_]: Async: Parallel](eventsClient: EventsClient[F]): Resource[F, EventsCached[F]] =
    for {
      cache <- Cache.expiring[F, String, Event](
        config = ExpiringCache.Config(
          expireAfterRead  = 3.hour,
          expireAfterWrite = Some(3.hour)
        )
      )
      logger <- Slf4jLogger.create[F].toResource
    } yield new EventsCached[F](logger, cache, eventsClient)
}
