package org.github.insider.polymarket.persistance

import cats.effect.kernel.Async
import doobie.Transactor
import org.github.insider.polymarket.domain.Market
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import cats.syntax.all._
import doobie.implicits._
import doobie.util.update.Update
import org.github.insider.polymarket.domain.Outcome.Other

import scala.collection.immutable.HashMap

class MarketsImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F]) extends Markets[F] {

  case class TradeForDb(
    conditionId: String,
    wallet: String,
    outcome: String,
    outcomeMessage: Option[String],
    tokenId: String,
    totalSize: BigDecimal,
    totalPrice: BigDecimal
  )

  object TradeForDb {
    def fromMarket(market: Market, values: (String, String, BigDecimal, BigDecimal)): TradeForDb = {
      val outcomeMessage = market.tokens.find(_.id == values._2).get.outcome match {
        case Other(value) => Some(value)
        case _            => None
      }
      TradeForDb(
        market.conditionId,
        values._1,
        market.tokens.find(_.id == values._2).get.outcome.toString,
        outcomeMessage,
        values._2,
        values._3,
        values._4
      )
    }
  }

  override def addMarket(market: Market, map: HashMap[(String, String), (BigDecimal, BigDecimal)]): F[Unit] = {
    val insertMarketConnection = sql"""
         |INSERT INTO markets (
         |    conditionId,
         |    id,
         |    question,
         |    volume,
         |    tokens
         |)
         |VALUES (
         |  ${market.conditionId},
         |  ${market.id},
         |  ${market.question},
         |  ${market.volume.value},
         |  ${market.tokens.map(_.id)}
         |);
       """.stripMargin.update.run.void

    val tradesForDb = map.toList.map(x => (x._1._1,x._1._1,x._2._1,x._2._2))
      .map(values => TradeForDb.fromMarket(market, values))
    val insertTradesConnection =
      Update[TradeForDb](
        """
          |INSERT INTO trades (
          | conditionId,
          | wallet,
          | outcome,
          | outcomeMessage,
          | tokenId,
          | totalSize,
          | totalPrice,
          |) VALUES (
          | ?,?,?,?,?,?,?
          |);
          |""".stripMargin
      ).updateMany(tradesForDb)

    (for {
      _ <- insertMarketConnection
      _ <- insertTradesConnection
    } yield ()).transact(transactor)
  }
}

object MarketsImpl {
  def of[F[_]: Async](transactor: Transactor[F]): F[Markets[F]] = {
    val loggerF = Slf4jLogger.create[F]
    loggerF.map(logger => new MarketsImpl[F](transactor, logger))
  }
}
