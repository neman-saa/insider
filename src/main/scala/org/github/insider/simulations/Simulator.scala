//package org.github.insider.simulations
//
//import cats.effect.Async
//import org.github.insider.alchemy.repository.TradesRepository
//import org.github.insider.leaderboard.LeaderboardStrategy.LeaderboardKeyName
//import org.github.insider.polymarket.repository.Events
//
//class Simulator[F[_]: Async](leaderboardKeyName: LeaderboardKeyName, trades: TradesRepository[F], events: Events[F]){
//  def simulate(maxForUser: BigDecimal, batchLength: Int, initialBalance: BigDecimal): F[BigDecimal] = {
//    def recursion(currentBalance: BigDecimal)
//  }
//}
