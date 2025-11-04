package org.github.insider.polymarket.configs

case class DbConfig(
  driver: String,
  nThreads: Int,
  host: String,
  port: String,
  name: String,
  sslModeRequire: Boolean,
  username: String,
  password: String,
  migrationsTable: String,
  migrationsLocations: List[String]
){
  def url: String = s"jdbc:postgresql://$host:$port/$name${if (sslModeRequire) "?sslmode=require" else ""}"
}
