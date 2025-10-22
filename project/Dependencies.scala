import sbt.*

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

  val dependencies: Seq[ModuleID] = Seq(
    "org.tpolecat"                  %% "doobie-core"                   % doobieVersion,
    "org.tpolecat"                  %% "doobie-hikari"                 % doobieVersion,
    "org.tpolecat"                  %% "doobie-postgres"               % doobieVersion,
    "org.tpolecat"                  %% "doobie-postgres-circe"         % doobieVersion,
    "org.tpolecat"                  %% "doobie-scalatest"              % doobieVersion,
    "org.typelevel"                 %% "cats-effect"                   % catsEffectVersion,
    "org.http4s"                    %% "http4s-dsl"                    % http4sVersion,
    "org.http4s"                    %% "http4s-ember-client"           % http4sVersion,
    "io.circe"                      %% "circe-fs2"                     % circeVersion,
    "io.circe"                      %% "circe-core"                    % circeVersion,
    "io.circe"                      %% "circe-generic"                 % circeVersion,
    "io.circe"                      %% "circe-generic-extras"          % circeVersion,
    "io.circe"                      %% "circe-optics"                  % circeVersion,
    "io.circe"                      %% "circe-parser"                  % circeVersion,
    "com.github.pureconfig"         %% "pureconfig"                    % pureConfigVersion,
    "org.typelevel"                 %% "log4cats-slf4j"                % log4catsVersion,
    "org.slf4j"                      % "slf4j-simple"                  % slf4jVersion,
    "org.flywaydb"                   % "flyway-core"                   % flywayVersion,
    "org.typelevel"                 %% "log4cats-noop"                 % log4catsVersion            % Test,
    "org.scalatest"                 %% "scalatest"                     % scalaTestVersion           % Test,
    "org.typelevel"                 %% "cats-effect-testing-scalatest" % scalaTestCatsEffectVersion % Test
  )
}