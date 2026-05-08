package org.github.insider.realtime.wallets

import scala.concurrent.duration.FiniteDuration

trait Trader[F[_]]{
  def performOperations: F[Unit]
  def updateEvery(duration: FiniteDuration): F[Unit]
}
