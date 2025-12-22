package org.github.insider.polymarket.configs

import org.github.insider.polymarket.configs.MainConfig.AlchemyConfig

final case class MainConfig(
  dbConfig: DbConfig,
  alchemy: AlchemyConfig,
)

object MainConfig {
  final case class AlchemyConfig(
    apiKey: String,
  )
}
