package org.github.insider.polymarket.configs

import cats.effect.kernel.{Resource, Sync}
import org.github.insider.polymarket.configs.MainConfig.{AlchemyConfig, PolymarketConfig, TelegramConfig}
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.semiauto.deriveReader

import java.util.Properties

final case class MainConfig(
  dbConfig: DbConfig,
  alchemy: AlchemyConfig,
  telegram: TelegramConfig,
  polymarket: PolymarketConfig
)

object MainConfig {
  final case class AlchemyConfig(
    apiKey: String,
    ctfAddress: String,
    collateralAddress: String,
    burnMintAddress: String
  )

  final case class TelegramConfig(token: String)

  case class PolymarketConfig(clobAddress: String, user: String)

  implicit val dbConfigReader: ConfigReader[DbConfig] = {
    implicit val PropertiesConfigReader: ConfigReader[Properties] =
      ConfigReader.fromCursor { c =>
        c.asMap.map { map =>
          val properties = new Properties()

          for {
            (key, configCursor) <- map
            configValue         <- configCursor.valueOpt
          } properties.put(key, configValue.unwrapped())

          properties
        }
      }

    deriveReader
  }
  implicit val alchemyConfigReader: ConfigReader[AlchemyConfig] = deriveReader

  implicit val mainConfigReader: ConfigReader[MainConfig] = deriveReader

  implicit val telegramConfigReader: ConfigReader[TelegramConfig] = deriveReader

  implicit val polymarketConfigReader: ConfigReader[PolymarketConfig] = deriveReader

  def loadR[F[_]: Sync]: Resource[F, MainConfig] =
    Resource.eval(Sync[F].delay(ConfigSource.default.loadOrThrow[MainConfig]))
}
