package org.github.insider.polymarket.workers

import cats.Parallel
import cats.effect.{Async, Ref}
import cats.syntax.all._
import org.github.insider.polymarket.client.TagsClient
import org.github.insider.polymarket.domain.Tag

private[workers] class TagsExtractorWorkerGroup[F[_]: Async: Parallel](
  tagsClient: TagsClient[F],
  relevantTags: Ref[F, List[Tag]],
  workersNumber: Int,
) {

  def getRelevantTags(keywords: List[String], limit: Int, maxDepth: Int): F[List[Tag]] =
    awaitedRunGroupOfN(keywords, limit, maxDepth) >> relevantTags.get

  private def awaitedRunGroupOfN(keywords: List[String], limit: Int, maxDepth: Int): F[Unit] = {
    for {
      offset <- Ref.of[F, Int](0)
      workers <-
        (1 to workersNumber)
          .map(n => TagsExtractorWorker.of[F](tagsClient, offset, relevantTags, n))
          .toList
          .sequence
      _ <- workers.parTraverse_(worker => worker.run(keywords, limit, maxDepth))
    } yield ()
  }
}

object TagsExtractorWorkerGroup {
  def of[F[_]: Async: Parallel](tagsClient: TagsClient[F])(workersNumber: Int = 1): F[TagsExtractorWorkerGroup[F]] =
    for {
      relevantTags <- Ref.of[F, List[Tag]](List.empty)
    } yield new TagsExtractorWorkerGroup[F](tagsClient, relevantTags, workersNumber)
}
