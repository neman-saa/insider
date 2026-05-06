package org.github.insider.polymarket.configs

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all._
import com.bot4s.telegram.models.ChatId
import org.github.insider.polymarket.configs.MainConfig.{
  AlchemyConfig,
  PolygonContracts,
  PolymarketConfig,
  TelegramConfig,
  WalletsConfig
}
import pureconfig.error.ExceptionThrown
import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.semiauto.deriveReader

import java.util.Properties
import scala.util.Try

final case class MainConfig(
  dbConfig: DbConfig,
  alchemy: AlchemyConfig,
  polygonContracts: PolygonContracts,
  telegram: TelegramConfig,
  polymarket: PolymarketConfig,
  wallets: WalletsConfig
)

object MainConfig {
  final case class AlchemyConfig(
    apiKey: String,
  )

  final case class PolygonContracts(
    standardCtf: String,
    negRiskCtf: String,
    filterOut: Set[String],
  ) {
    val ctfs: Set[String] = Set(standardCtf, negRiskCtf)
  }

  final case class TelegramConfig(
    chatId: ChatId,
    botToken: String
  )

  final case class WalletsConfig(
    marketsAmount: Int,
    secondsToSellBeforeResolve: Int,
    sellThresholdPercent: Int,
    spreadPercent: Int,
    updateEveryMinutes: Int,
    checkLastNBlocks: Int
  )

  case class PolymarketConfig(clobAddress: String, userAddress: String, bearer: String)

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
  implicit val alchemyConfigReader: ConfigReader[AlchemyConfig]             = deriveReader
  implicit val polygonContractsConfigReader: ConfigReader[PolygonContracts] = deriveReader

  implicit val mainConfigReader: ConfigReader[MainConfig] = deriveReader

  implicit val telegramConfigReader: ConfigReader[TelegramConfig] = {
    implicit val ChatIdReader: ConfigReader[ChatId] =
      ConfigReader.fromString(chatIdStr => Try(ChatId(chatIdStr.toLong)).toEither.leftMap(ExceptionThrown))

    deriveReader
  }

  implicit val polymarketConfigReader: ConfigReader[PolymarketConfig] = deriveReader
  implicit val walletsConfigReader: ConfigReader[WalletsConfig]       = deriveReader

  def loadR[F[_]: Sync]: Resource[F, MainConfig] =
    Resource.eval(Sync[F].delay(ConfigSource.default.loadOrThrow[MainConfig]))
}
