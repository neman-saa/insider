package org.github.insider.realtime.traders.scorevector

import cats.effect.Ref
import cats.effect.kernel.Sync
import cats.syntax.all._
import org.github.insider.realtime.tokens.TokenId

class OperationsAuditor[F[_]: Sync](
  auditsR: Ref[F, Map[TokenId, List[OperationAuditLog]]],
  operationsAuditRepository: OperationsAuditRepository[F],
) {

  def lastAuditLogFor(tokenId: TokenId): F[Option[OperationAuditLog]] =
    auditsR.get.map { audits =>
      audits.get(tokenId).map(logs => logs.maxBy(_.timestamp))
    }

  def audit(log: OperationAuditLog): F[Unit] =
    operationsAuditRepository.insert(log) >> auditsR.update { audits =>
      audits.updated(log.tokenId, audits.getOrElse(log.tokenId, Nil).appended(log))
    }
}

object OperationsAuditor {
  def of[F[_]: Sync](operationsAuditRepository: OperationsAuditRepository[F]): F[OperationsAuditor[F]] =
    for {
      allAudits <- operationsAuditRepository.selectAll
      audits     = allAudits.groupBy(_.tokenId)
      auditsR   <- Ref.of[F, Map[TokenId, List[OperationAuditLog]]](audits)
    } yield new OperationsAuditor[F](auditsR, operationsAuditRepository)
}
