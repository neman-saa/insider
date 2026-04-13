package org.github.insider.simulations

import cats.effect.{IO, IOApp}
import org.github.insider.polymarket.configs.MainConfig
import cats.syntax.all._
import org.github.insider.alchemy.repository.TradesRepositoryImpl
import org.github.insider.leaderboard.{
  LeaderboardStrategy,
  Leaderboards,
  RoiNoTradersStrategyCh,
  WinRateLeaderboardStrategyCH
}
import org.github.insider.persistance.Database
import org.typelevel.log4cats.slf4j.Slf4jLogger

object SimulatorMain extends IOApp.Simple {

  private final val config =
    SimulationConfig(
      blocksProcessingBatchSize = 1000,
      leaderboardLimit          = 1000,
      leaderboardSecondsLifetime = 3600 * 24,
      initialWalletBalance      = 1000,
      minWalletBlocksLifetime   = 60_000,
      maxWalletBlocksLifetime   = 200_000,
      maxTemporaryWalletsInPool = 3
    )

  override def run: IO[Unit] = {
    val resource = for {
      config     <- MainConfig.loadR[IO]
      transactor <- Database.makeTransactor[IO](config.dbConfig)

      simulationsRepository <- SimulationsRepository.of[IO](transactor).toResource

      leaderboard = RoiNoTradersStrategyCh[IO](transactor)

      simulator <- Simulator.of[IO](leaderboard, simulationsRepository).toResource
    } yield simulator

    resource.use { simulator =>
      for {
        logger <- Slf4jLogger.create[IO]
        _      <- logger.info("Simulation application started after successful resource acquisition...")

        _ <- simulator.start(80_000_000, 84_000_000)(config)
      } yield ()
    }
  }
}
