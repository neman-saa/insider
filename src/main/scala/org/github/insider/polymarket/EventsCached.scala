package org.github.insider.polymarket

import cats.Parallel
import cats.effect.Async
import cats.effect.implicits.effectResourceOps
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.evolution.scache.{Cache, ExpiringCache}
import org.github.insider.polymarket.client.EventsClient
import org.github.insider.polymarket.domain.Event
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

class EventsCached[F[_]: Async](logger: Logger[F], cache: Cache[F, String, Event], eventsClient: EventsClient[F]) {

  def find(tokenIds: List[String]): F[Map[String, Event]] = for {
    contained <- tokenIds.traverse(tokenId => cache.contains(tokenId).map((tokenId, _)))
    needToLoad = contained.collect { case (tokenId, false) => tokenId }
    events    <- eventsClient.getEventsByTokens(needToLoad)
    map = needToLoad
      .map(tokenId => tokenId -> events.find(_.markets.get.head.tokens.flatMap(_.id).contains(tokenId)).get)
      .toMap
    res <- tokenIds.traverse { tokenId =>
      cache.getOrUpdate(tokenId)(map(tokenId).pure[F]).map(tokenId -> _)
    }
  } yield res.toMap
}

object EventsCached {
  def of[F[_]: Async: Parallel](eventsClient: EventsClient[F]): Resource[F, EventsCached[F]] =
    for {
      cache <- Cache.expiring[F, String, Event](
        config = ExpiringCache.Config(
          expireAfterRead  = 1.hour,
          expireAfterWrite = Some(10.minute)
        )
      )
      logger <- Slf4jLogger.create[F].toResource
    } yield new EventsCached[F](logger, cache, eventsClient)
}
