import Dependencies._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

lazy val root = (project in file("."))
  .settings(
    name := "insider",
    libraryDependencies ++= Seq(
      catsEffect,
      pureConfig,
      logger,
      apacheLog4j,
      migrations,
      Testing.log      % Test,
      Testing.test     % Test,
      Testing.catsTest % Test,
      Http4s.dsl,
      Http4s.client,
      Http4s.circe,
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
  )
