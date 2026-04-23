package org.github.insider.realtime.tokens

import cats.effect.implicits.genSpawnOps
import cats.effect.{Clock, Ref}
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all._
import org.github.insider.leaderboard.HexAddress
import org.github.insider.leaderboard.LeaderboardEntry.AdvancedLeaderboardEntry
import org.github.insider.polymarket.domain.Trade

import scala.concurrent.duration.FiniteDuration

final class TokensInfoRegistry[F[_]: Sync] private (registryR: Ref[F, Map[TokenId, TokenInfo]]) {

  /**
    * @return
    *   List of updated tokens info (including opposite tokens) for trades with known meta info, empty list otherwise
    */
  def updateWith(
    trades: List[Trade],
    tokensMetaInfo: Map[TokenId, TokenMetaInfo],
    leaderboard: Map[HexAddress, AdvancedLeaderboardEntry],
  ): F[List[TokenInfo]] = {
    trades.flatTraverse { trade =>
      registryR.modify { registry =>
        tokensMetaInfo.get(trade.tokenId) match {
          case Some(metaInfo) =>
            val scoreFromLeader = leaderboard.get(HexAddress(trade.makerAddress)) match {
              case Some(entry) =>
                val score =
                  trade.totalPrice / entry.avgBuy * entry.score / entry.totalLeaderboardScore * entry.totalLeaderboardSize
                score * trade.side.sign
              case None => BigDecimal(0)
            }

            val tokenScore         = registry.get(trade.tokenId).map(_.score).getOrElse(BigDecimal(0))
            val oppositeTokenScore = registry.get(metaInfo.oppositeTokenId).map(_.score).getOrElse(BigDecimal(0))

            val updatedTokenInfo = TokenInfo(
              id               = trade.tokenId,
              price            = trade.singleTokenPrice,
              score            = tokenScore + scoreFromLeader,
              resolveDate      = metaInfo.resolveDate,
              lastUpdatedBlock = trade.blockNum,
            )
            val updatedOppositeTokenInfo = TokenInfo(
              id               = metaInfo.oppositeTokenId,
              price            = 1 - trade.singleTokenPrice,
              score            = oppositeTokenScore - scoreFromLeader,
              resolveDate      = metaInfo.resolveDate,
              lastUpdatedBlock = trade.blockNum,
            )

            val updatedRegistry =
              registry + (trade.tokenId -> updatedTokenInfo) + (metaInfo.oppositeTokenId -> updatedOppositeTokenInfo)

            (updatedRegistry, List(updatedTokenInfo, updatedOppositeTokenInfo))
          case None =>
            (registry, List.empty)
        }
      }
    }
  }

  def topTokens(limit: Int): F[Map[TokenId, BigDecimal]] = {
    for {
      now      <- Clock[F].realTimeInstant
      registry <- registryR.get
    } yield {
      registry
        .map {
          case (tokenId, tokenInfo) =>
            val timeToResolve = (tokenInfo.resolveDate.getEpochSecond - now.getEpochSecond).max(1)
            val efficiency    = (1 - tokenInfo.price) * tokenInfo.score / timeToResolve

            tokenId -> (tokenInfo.price, efficiency)
        }
        .toList
        .filter {
          case (_, (_, efficiency)) => efficiency > 0
        }
        .sortBy {
          case (_, (_, efficiency)) => -efficiency
        }
        .take(limit)
        .map { case (tokenId, (price, _)) => tokenId -> price }
        .toMap
    }
  }

  private def cleanUpAction: F[Unit] =
    Clock[F].realTimeInstant.flatMap { now =>
      registryR.update { registry =>
        registry.filter {
          case (_, tokenInfo) => tokenInfo.resolveDate.isAfter(now)
        }
      }
    }
}

object TokensInfoRegistry {
  def withInit[F[_]: Async](
    tokensInfoRepository: TokensInfoRepository[F],
    cleanUpPeriod: FiniteDuration,
  ): F[TokensInfoRegistry[F]] =
    for {
      now          <- Clock[F].realTimeInstant
      tokensInfo   <- tokensInfoRepository.select(now)
      registryR    <- Ref.of(tokensInfo.map(info => info.id -> info).toMap)
      tokenRegistry = new TokensInfoRegistry[F](registryR)
      _            <- fs2.Stream.repeatEval(tokenRegistry.cleanUpAction).metered(cleanUpPeriod).compile.drain.start
    } yield tokenRegistry
}
