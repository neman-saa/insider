import sbt._

object Dependencies {
  lazy val circeVersion               = "0.14.1"
  lazy val catsEffectVersion          = "3.3.14"
  lazy val http4sVersion              = "0.23.18"
  lazy val doobieVersion              = "1.0.0-RC4"
  lazy val pureConfigVersion          = "0.17.6"
  lazy val log4catsVersion            = "2.4.0"
  lazy val scalaTestVersion           = "3.2.12"
  lazy val scalaTestCatsEffectVersion = "1.4.0"
  lazy val slf4jVersion               = "2.0.0"
  lazy val flywayVersion              = "9.16.0"

  private object Circe {
    val circeFs2 = "io.circe" %% "circe-fs2"            % circeVersion
    val core     = "io.circe" %% "circe-core"           % circeVersion
    val generic  = "io.circe" %% "circe-generic"        % circeVersion
    val extras   = "io.circe" %% "circe-generic-extras" % circeVersion
    val optics   = "io.circe" %% "circe-optics"         % circeVersion
    val parser   = "io.circe" %% "circe-parser"         % circeVersion
  }

  private object Doobie {
    val core          = "org.tpolecat" %% "doobie-core"           % doobieVersion
    val hikari        = "org.tpolecat" %% "doobie-hikari"         % doobieVersion
    val postgres      = "org.tpolecat" %% "doobie-postgres"       % doobieVersion
    val postgresCirce = "org.tpolecat" %% "doobie-postgres-circe" % doobieVersion
    val test          = "org.tpolecat" %% "doobie-scalatest"      % doobieVersion
  }

  private object Http4s {
    val dsl    = "org.http4s" %% "http4s-dsl"          % http4sVersion
    val client = "org.http4s" %% "http4s-ember-client" % http4sVersion
  }

  private object Testing {
    val log      = "org.typelevel" %% "log4cats-noop"                 % log4catsVersion
    val test     = "org.scalatest" %% "scalatest"                     % scalaTestVersion
    val catsTest = "org.typelevel" %% "cats-effect-testing-scalatest" % scalaTestCatsEffectVersion
  }
  val dependencies: Seq[ModuleID] = Seq(
    "org.typelevel"         %% "cats-effect"    % catsEffectVersion,
    "com.github.pureconfig" %% "pureconfig"     % pureConfigVersion,
    "org.typelevel"         %% "log4cats-slf4j" % log4catsVersion,
    "org.slf4j"              % "slf4j-simple"   % slf4jVersion,
    "org.flywaydb"           % "flyway-core"    % flywayVersion,
    Testing.log % Test,
    Testing.test % Test,
    Testing.catsTest % Test,
    Http4s.dsl,
    Http4s.client,
    Doobie.core,
    Doobie.hikari,
    Doobie.postgres,
    Doobie.postgresCirce,
    Doobie.test,
    Circe.circeFs2,
    Circe.core,
    Circe.extras,
    Circe.generic,
    Circe.optics,
    Circe.parser
  )
}
