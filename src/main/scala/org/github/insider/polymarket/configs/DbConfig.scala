package org.github.insider.polymarket.configs

import java.util.Properties

final case class DbConfig(
  driver: String,
  nThreads: Int,
  host: String,
  port: String,
  name: String,
  username: String,
  password: String,
  migrationsTable: String,
  migrationsLocations: List[String],
  properties: Properties,
) {
  def url: String = s"jdbc:clickhouse://$host:$port/$name"
}
