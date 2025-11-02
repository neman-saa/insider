package org.github.insider.polymarket.workers

import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.polymarket.client.TagsClient
import org.github.insider.polymarket.domain.Tag
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

private[workers] class TagsExtractorWorker[F[_]: Async](
  tagsClient: TagsClient[F],
  offset: Ref[F, Int],
  collectedTags: Ref[F, List[Tag]],
  logger: Logger[F],
)(workerNumber: Int) {

  def run(keywords: List[String], limit: Int, maxDepth: Int): F[Unit] =
    offset.getAndUpdate(_ + limit).flatMap {
      case currentOffset if maxDepth < currentOffset => Async[F].unit
      case currentOffset =>
        val action =
          for {
            tags        <- tagsClient.getTags(limit, currentOffset)
            filteredTags = filterTagsByKeywords(tags, keywords)
            _ <- logger
              .info(
                s"[Worker - $workerNumber] Tags received: ${tags.length}, proper tags: ${filteredTags.length}, total: $currentOffset"
              )
            _ <- collectedTags.update(_.appendedAll(filteredTags))
          } yield ()

        action >> run(keywords, limit, maxDepth)
    }

  private def filterTagsByKeywords(tags: List[Tag], keywords: List[String]): List[Tag] =
    tags.filter { tag =>
      keywords.exists(keyword => tag.label.exists(_.toLowerCase.contains(keyword)))
    }
}

private[workers] object TagsExtractorWorker {
  def of[F[_]: Async](
    tagsClient: TagsClient[F],
    offset: Ref[F, Int],
    relevantTags: Ref[F, List[Tag]],
    workerN: Int,
  ): F[TagsExtractorWorker[F]] =
    Slf4jLogger.create[F].map(logger => new TagsExtractorWorker[F](tagsClient, offset, relevantTags, logger)(workerN))
}
