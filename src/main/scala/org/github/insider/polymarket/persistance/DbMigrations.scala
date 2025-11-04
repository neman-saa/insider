package org.github.insider.polymarket.persistance

import cats.effect.Sync
import cats.implicits._
import org.flywaydb.core.api.configuration.FluentConfiguration
import org.flywaydb.core.api.Location
import org.flywaydb.core.Flyway
import org.github.insider.polymarket.configs.DbConfig
import org.typelevel.log4cats.slf4j.Slf4jLogger

object DbMigrations {

  def migrate[F[_]: Sync](config: DbConfig): F[Int] = {
    val loggerF = Slf4jLogger.create[F]
    loggerF.flatMap{ logger =>
      logger.info("Running migrations from locations: " +
        config.migrationsLocations.mkString(", ")) >> {
        val count = unsafeMigrate(config)
        logger.info(s"Executed $count migrations") >> count.pure[F]
      }
    }

  }

  private def unsafeMigrate(config: DbConfig): Int = {
    val m: FluentConfiguration = Flyway.configure
      .dataSource(
        config.url,
        config.username,
        config.password
      )
      .group(true)
      .outOfOrder(false)
      .table(config.migrationsTable)
      .locations(
        config.migrationsLocations
          .map(new Location(_)): _*
      )
      .baselineOnMigrate(true)

    m.load().migrate().migrationsExecuted
  }

}
