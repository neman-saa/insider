package org.github.insider.realtime.tokens

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
  lastNBlocksScores: Ref[F, List[List[(String, BigDecimal)]]],
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
    val scoresFromTrades = trades.collect {
      case trade if leaderboard.contains(HexAddress(trade.makerAddress)) =>
        val entry = leaderboard(HexAddress(trade.makerAddress))
        val score =
          trade.totalPrice / entry.avgBuy * entry.score / entry.totalLeaderboardScore * entry.totalLeaderboardSize
        trade.tokenId -> score * trade.side.sign
    }
    lastNBlocksScores.update(scores => scores.tail :+ scoresFromTrades)
  } >>
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
  def setBuyTimeBuyPrice(tokenId: TokenId, buyTime: Instant, buyPrice: BigDecimal): F[Unit] =
    registryR.update { map =>
      val info    = map(tokenId)
      val newInfo = info.copy(buyPrice = Some(buyPrice), buyTime = Some(buyTime))
      map + (tokenId -> newInfo)
    } >> tokenInfos.setBuyPriceTime(tokenId, buyPrice, buyTime)

  def topTokensInfo: F[List[TokenInfoShort]] = {
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
          case (_, tokenInfo) =>
            val timeToResolve =
              (tokenInfo.resolveDate.getEpochSecond - now.getEpochSecond).max(1)
            val efficiency = (1 - tokenInfo.price) * tokenInfo.score / timeToResolve

            TokenInfoShort(
              tokenInfo.id,
              efficiency,
              tokenInfo.buyTime,
              tokenInfo.price,
              tokenInfo.resolveDate,
              tokenInfo.score
            )
        }
        .toList
        .filter {
          case TokenInfoShort(_, efficiency, _, _, _, _) => efficiency > 0
        }
        .sortBy {
          case TokenInfoShort(_, efficiency, _, _, _, _) => -efficiency
        }
        .take(marketsAmount)
    }
  }

  def tokensInfoForTokens(tokens: List[String]): F[List[TokenInfoShort]] = {
    Clock[F]
      .realTimeInstant
      .flatMap(now =>
        tokenInfos
          .getForTokens(tokens)
          .map(_.map {
            case (tokenId, tokenInfo) =>
              val timeToResolve =
                tokenInfo.resolveDate.getEpochSecond - now.getEpochSecond
              val efficiency = (1 - tokenInfo.price) * tokenInfo.score / timeToResolve.max(1)

              TokenInfoShort(
                tokenId,
                efficiency,
                tokenInfo.buyTime,
                tokenInfo.price,
                tokenInfo.resolveDate,
                tokenInfo.score
              )
          }.toList)
      )
  }

  def latestScores: F[Map[TokenId, BigDecimal]] = for {
    latestScores <- lastNBlocksScores.get
    summedScores  = latestScores.flatten.groupBy(_._1).view.mapValues(_.map(_._2).sum).toMap
  } yield summedScores // only scores

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
    marketsAmount: Int,
    lastNBlocks: Int
  ): F[TokensInfoRegistry[F]] =
    for {
      now          <- Clock[F].realTimeInstant
      tokensInfo   <- tokensInfoRepository.select(now)
      latestScores <- Ref.of(List.fill(lastNBlocks)(List.empty[(String, BigDecimal)]))
      registryR    <- Ref.of(tokensInfo.map(info => info.id -> info).toMap)
      tokenRegistry = new TokensInfoRegistry[F](
        registryR,
        latestScores,
        secondsToSellBeforeResolve,
        tokensInfoRepository,
        marketsAmount,
      )
      _ <- fs2.Stream.repeatEval(tokenRegistry.cleanUpAction).metered(cleanUpPeriod).compile.drain.start
    } yield tokenRegistry
}
