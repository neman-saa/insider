package org.github.insider.persistance

import org.github.insider.polymarket.configs.DbConfig
import cats.effect.Async
import cats.effect.Resource
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts

object Database {

  def postgresResource[F[_]: Async](config: DbConfig): Resource[F, HikariTransactor[F]] = for {
    ec <- ExecutionContexts.fixedThreadPool(config.nThreads)
    xa <- HikariTransactor.newHikariTransactor[F](
      config.driver,
      config.url,
      config.username,
      config.password,
      ec
    )
  } yield xa
}
