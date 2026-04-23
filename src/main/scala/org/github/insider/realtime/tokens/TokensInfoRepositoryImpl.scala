package org.github.insider.realtime.tokens

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._
import doobie.Transactor
import doobie.util.update.Update
import org.typelevel.log4cats.Logger
import doobie.syntax.all._
import doobie.postgres.implicits._
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.Instant

class TokensInfoRepositoryImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F])
    extends TokensInfoRepository[F] {

  override def insert(tokens: NonEmptyList[TokenInfo]): F[Unit] =
    Update[TokenInfo](
      """
        |INSERT INTO tokens_info (id, price, score, resolve_date, last_updated_block_num)
        |VALUES (?, ?, ?, ?, ?)
        |""".stripMargin
    )
      .updateMany(tokens)
      .transact(transactor)
      .void
      .flatTap(rows => logger.info(s"Inserted $rows tokens info in 'tokens_info' table"))

  override def select(now: Instant): F[List[TokenInfo]] =
    fr"""
        |SELECT id, price, score, resolve_date, last_updated_block_num
        |FROM tokens_info
        |WHERE resolve_date > $now
        |"""
      .stripMargin
      .query[TokenInfo]
      .to[List]
      .transact(transactor)
}

object TokensInfoRepositoryImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[TokensInfoRepository[F]] =
    Slf4jLogger.create[F].map(logger => new TokensInfoRepositoryImpl[F](transactor, logger))
}
