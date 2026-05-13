package org.github.insider.realtime.traders.scorevector

import cats.effect.kernel.Async
import cats.syntax.all._
import doobie.util.fragment.Fragment
import doobie.util.transactor.Transactor
import doobie.implicits.javatimedrivernative._
import doobie.syntax.all._
import org.github.insider.polymarket.domain.Side
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

import java.time.ZonedDateTime

trait OperationsAuditRepository[F[_]] {
  def insert(auditLog: OperationAuditLog): F[Unit]
  def selectAll: F[List[OperationAuditLog]]
}

object OperationsAuditRepository {

  class OperationsAuditRepositoryImpl[F[_]: Async](transactor: Transactor[F], logger: Logger[F])
      extends OperationsAuditRepository[F] {
    override def insert(auditLog: OperationAuditLog): F[Unit] =
      insertQuery(auditLog)
        .update
        .run
        .transact(transactor)
        .flatTap { n =>
          logger.info(s"Inserted $n logs into 'operations_audit' table")
        }
        .void

    private def insertQuery(auditLog: OperationAuditLog): Fragment =
      fr"""
        |INSERT INTO operations_audit (
        |   token_id, 
        |   side,
        |   moment_score, 
        |   total_price, 
        |   total_shares, 
        |   prev_single_token_price, 
        |   single_token_price,
        |   timestamp
        |)
        |VALUES (
        |   ${auditLog.tokenId}, 
        |   ${auditLog.side}, 
        |   ${auditLog.momentScore}, 
        |   ${auditLog.totalPrice}, 
        |   ${auditLog.totalShares}, 
        |   ${auditLog.prevSingleTokenPrice}, 
        |   ${auditLog.singleTokenPrice}, 
        |   ${auditLog.timestamp}
        |)
        |""".stripMargin

    override def selectAll: F[List[OperationAuditLog]] =
      selectAllQuery
        .query[
          (String, Side, Option[BigDecimal], BigDecimal, BigDecimal, Option[BigDecimal], BigDecimal, ZonedDateTime)
        ]
        .map[OperationAuditLog] {
          case (
                tokenId,
                Side.Buy,
                momentScore,
                totalPrice,
                totalShares,
                prevSingleTokenPrice,
                singleTokenPrice,
                timestamp
              ) =>
            OperationAuditLog.BuyAuditLog(
              tokenId,
              momentScore,
              totalPrice,
              totalShares,
              prevSingleTokenPrice,
              singleTokenPrice,
              timestamp.toInstant
            )
          case (
                tokenId,
                Side.Sell,
                momentScore,
                totalPrice,
                totalShares,
                prevSingleTokenPrice,
                singleTokenPrice,
                timestamp
              ) =>
            OperationAuditLog.SellAuditLog(
              tokenId,
              momentScore,
              totalPrice,
              totalShares,
              prevSingleTokenPrice,
              singleTokenPrice,
              timestamp.toInstant
            )
        }
        .to[List]
        .transact(transactor)

    private def selectAllQuery: Fragment =
      fr"""
        |SELECT
        |   token_id, 
        |   side,
        |   moment_score, 
        |   total_price, 
        |   total_shares, 
        |   prev_single_token_price, 
        |   single_token_price,
        |   timestamp
        |FROM operations_audit
        |""".stripMargin
  }

  object OperationsAuditRepositoryImpl {
    def of[F[_]: Async](transactor: Transactor[F]): F[OperationsAuditRepository[F]] = {
      val loggerF = Slf4jLogger.create[F]
      loggerF.map(logger => new OperationsAuditRepositoryImpl[F](transactor, logger))
    }
  }
}
