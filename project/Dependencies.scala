import sbt._

object Dependencies {
  private val catsEffectVersion = "3.3.14"
  private val pureConfigVersion = "0.17.6"
  private val slf4jVersion      = "2.0.0"
  private val flywayVersion     = "9.16.0"
  private val log4catsVersion   = "2.4.0"
  private val telegramBotClient = "0.6.0"

  object TelegramBot {
    val client = "org.augustjune" %% "canoe" % telegramBotClient
  }

  object Circe {
    private val circeVersion = "0.14.1"

    val circeFs2 = "io.circe" %% "circe-fs2"            % circeVersion
    val core     = "io.circe" %% "circe-core"           % circeVersion
    val generic  = "io.circe" %% "circe-generic"        % circeVersion
    val extras   = "io.circe" %% "circe-generic-extras" % circeVersion
    val optics   = "io.circe" %% "circe-optics"         % circeVersion
    val parser   = "io.circe" %% "circe-parser"         % circeVersion
  }

  object Doobie {
    private val doobieVersion = "1.0.0-RC4"

    val core          = "org.tpolecat" %% "doobie-core"           % doobieVersion
    val hikari        = "org.tpolecat" %% "doobie-hikari"         % doobieVersion
    val postgres      = "org.tpolecat" %% "doobie-postgres"       % doobieVersion
    val postgresCirce = "org.tpolecat" %% "doobie-postgres-circe" % doobieVersion
    val test          = "org.tpolecat" %% "doobie-scalatest"      % doobieVersion
  }

  object Http4s {
    private val http4sVersion = "0.23.18"

    val dsl    = "org.http4s" %% "http4s-dsl"          % http4sVersion
    val client = "org.http4s" %% "http4s-ember-client" % http4sVersion
    val circe  = "org.http4s" %% "http4s-circe"        % http4sVersion
  }

  object Testing {
    private val scalaTestVersion           = "3.2.12"
    private val scalaTestCatsEffectVersion = "1.4.0"

    val log      = "org.typelevel" %% "log4cats-noop"                 % log4catsVersion
    val test     = "org.scalatest" %% "scalatest"                     % scalaTestVersion
    val catsTest = "org.typelevel" %% "cats-effect-testing-scalatest" % scalaTestCatsEffectVersion
  }

  val catsEffect  = "org.typelevel"           %% "cats-effect"       % catsEffectVersion
  val pureConfig  = "com.github.pureconfig"   %% "pureconfig"        % pureConfigVersion
  val logger      = "org.typelevel"           %% "log4cats-slf4j"    % log4catsVersion
  val apacheLog4j = "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.24.0"
  val migrations  = "org.flywaydb"             % "flyway-core"       % flywayVersion
  val clickhouse  = "com.clickhouse"           % "clickhouse-jdbc"   % "0.8.2"
}
