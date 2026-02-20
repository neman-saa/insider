package org.github.insider.persistance

import org.github.insider.polymarket.configs.DbConfig
import cats.effect.{Async, Resource, Sync}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import cats.syntax.all._

object Database {
  def makeTransactor[F[_]: Async](config: DbConfig): Resource[F, HikariTransactor[F]] =
    ExecutionContexts
      .fixedThreadPool[F](config.nThreads)
      .flatMap { ec =>
        HikariTransactor.newHikariTransactor[F](
          config.driver,
          config.url,
          config.username,
          config.password,
          ec
        )
      }
      .evalMap { transactor =>
        transactor.configure { dataSource =>
          Sync[F].delay {
            dataSource.setConnectionTestQuery("SELECT 1")
            dataSource.setDataSourceProperties(config.properties)
            dataSource.setAutoCommit(true)
          }
        } as transactor
      }
}
