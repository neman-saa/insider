package org.github.insider.realtime.tokens

import cats.data.NonEmptyList
import cats.effect.implicits.genSpawnOps
import cats.effect.{Clock, Ref}
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all._
import org.github.insider.leaderboard.HexAddress
import org.github.insider.leaderboard.LeaderboardEntry.AdvancedLeaderboardEntry
import org.github.insider.polymarket.domain.Trade

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

final class TokensInfoRegistry[F[_]: Sync] private (
  registryR: Ref[F, Map[TokenId, TokenInfo]],
  secondsToSellBeforeResolve: Int,
  tokenInfos: TokensInfoRepository[F],
  marketsAmount: Int
) {

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

            val tokenInfo         = registry.get(trade.tokenId)
            val oppositeTokenInfo = registry.get(metaInfo.oppositeTokenId)

            val updatedTokenInfo = TokenInfo(
              id               = trade.tokenId,
              price            = trade.singleTokenPrice,
              score            = tokenInfo.fold(BigDecimal(0))(_.score) + scoreFromLeader,
              resolveDate      = metaInfo.resolveDate,
              lastUpdatedBlock = trade.blockNum,
              buyPrice         = tokenInfo.flatMap(_.buyPrice),
              buyTime          = tokenInfo.flatMap(_.buyTime)
            )
            val updatedOppositeTokenInfo = TokenInfo(
              id               = metaInfo.oppositeTokenId,
              price            = 1 - trade.singleTokenPrice,
              score            = oppositeTokenInfo.fold(BigDecimal(0))(_.score) - scoreFromLeader,
              resolveDate      = metaInfo.resolveDate,
              lastUpdatedBlock = trade.blockNum,
              buyPrice         = oppositeTokenInfo.flatMap(_.buyPrice),
              buyTime          = oppositeTokenInfo.flatMap(_.buyTime)
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

  def setBuyTimeBuyPrice(tokenId: TokenId, buyTime: Instant, buyPrice: BigDecimal): F[Unit] =
    registryR.update { map =>
      val info    = map(tokenId)
      val newInfo = info.copy(buyPrice = Some(buyPrice), buyTime = Some(buyTime))
      map + (tokenId -> newInfo)
    } >> tokenInfos.setBuyPriceTime(tokenId, buyPrice, buyTime)

  def topTokensInfo: F[List[(TokenId, TokenInfoShort)]] = {
    for {
      now      <- Clock[F].realTimeInstant
      registry <- registryR.get
    } yield {
      registry
        .filter {
          case (_, tokenInfo) =>
            tokenInfo.resolveDate.getEpochSecond - secondsToSellBeforeResolve > now.getEpochSecond
        }
        .map {
          case (tokenId, tokenInfo) =>
            val timeToResolve =
              (tokenInfo.resolveDate.getEpochSecond - now.getEpochSecond - secondsToSellBeforeResolve).max(1)
            val efficiency = (1 - tokenInfo.price) * tokenInfo.score / timeToResolve

            tokenId -> TokenInfoShort(tokenInfo.id, efficiency, tokenInfo.buyTime, tokenInfo.price)
        }
        .toList
        .filter {
          case (_, TokenInfoShort(_, efficiency, _, _)) => efficiency > 0
        }
        .sortBy {
          case (_, TokenInfoShort(_, efficiency, _, _)) => -efficiency
        }
        .take(marketsAmount)
    }
  }

  def tokensInfoForTokens(tokens: NonEmptyList[String]): F[Map[TokenId, TokenInfoShort]] = {
    Clock[F]
      .realTimeInstant
      .flatMap(now =>
        tokenInfos
          .getForTokens(tokens)
          .map(_.map {
            case (tokenId, tokenInfo) =>
              val timeToResolve =
                tokenInfo.resolveDate.getEpochSecond - now.getEpochSecond - secondsToSellBeforeResolve
              val efficiency = (1 - tokenInfo.price) * tokenInfo.score / timeToResolve *
                (if (tokenInfo.score < 0 && timeToResolve < 0) -1 else 1)

              tokenId -> TokenInfoShort(tokenId, efficiency, tokenInfo.buyTime, tokenInfo.price)
          })
      )
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
    secondsToSellBeforeResolve: Int,
    marketsAmount: Int
  ): F[TokensInfoRegistry[F]] =
    for {
      now        <- Clock[F].realTimeInstant
      tokensInfo <- tokensInfoRepository.select(now)
      registryR  <- Ref.of(tokensInfo.map(info => info.id -> info).toMap)
      tokenRegistry = new TokensInfoRegistry[F](
        registryR,
        secondsToSellBeforeResolve,
        tokensInfoRepository,
        marketsAmount
      )
      _ <- fs2.Stream.repeatEval(tokenRegistry.cleanUpAction).metered(cleanUpPeriod).compile.drain.start
    } yield tokenRegistry
}
