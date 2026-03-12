package org.github.insider.polymarket

import cats.Parallel
import cats.effect.Async
import cats.effect.implicits.effectResourceOps
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.evolution.scache.{Cache, ExpiringCache}
import org.github.insider.polymarket.client.EventsClient
import org.github.insider.polymarket.domain.Event
import org.typelevel.log4cats.slf4j.Slf4jLogger

import scala.concurrent.duration.DurationInt

class EventsCached[F[_]: Async](cache: Cache[F, String, Event], eventsClient: EventsClient[F]) {

  def load(tokenId: String): F[Event] = eventsClient.getEventByToken(tokenId).map(_.get)
  def find(tokenId: String): F[Event] = cache.getOrUpdate(tokenId)(load(tokenId))
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
    } yield new EventsCached[F](cache, eventsClient)
}
