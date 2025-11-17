package org.github.insider.polymarket.configs

import scala.reflect.ClassTag

import cats.implicits._
import cats.MonadThrow
import pureconfig.error.ConfigReaderException
import pureconfig.ConfigReader
import pureconfig.ConfigSource

object syntax {
  implicit class sourceOps(source: ConfigSource) {
    def loadF[F[_], A](implicit reader: ConfigReader[A], F: MonadThrow[F]): F[A] =
      F.pure(source.load[A]).flatMap {
        case Left(errors) => F.raiseError[A](ConfigReaderException(errors))

        case Right(value) => F.pure(value)
      }
  }
}
