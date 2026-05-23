package org.github.insider.realtime.tokens

import cats.effect.implicits.genSpawnOps
import cats.effect.{Clock, Ref}
import cats.effect.kernel.{Async, Sync}
import cats.syntax.all._
import org.github.insider.leaderboard.HexAddress
import org.github.insider.leaderboard.LeaderboardEntry.AdvancedLeaderboardEntry
import org.github.insider.polymarket.domain.Trade

import scala.concurrent.duration.FiniteDuration

final class TokensInfoRegistry[F[_]: Sync] private (
  registryR: Ref[F, Map[TokenId, TokenInfo]],
  recentScoreChangesR: Ref[F, List[Map[TokenId, BigDecimal]]],
  secondsToSellBeforeResolve: Int,
  recentScoreChangesBlocksLength: Int,
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
    for {
      _             <- updateRecentScoreChanges(trades, leaderboard, tokensMetaInfo)
      updatedTokens <- updateRegistry(trades, tokensMetaInfo, leaderboard)
    } yield updatedTokens
  }

  private def updateRecentScoreChanges(
    trades: List[Trade],
    leaderboard: Map[HexAddress, AdvancedLeaderboardEntry],
    tokensMetaInfo: Map[TokenId, TokenMetaInfo]
  ): F[Unit] = {
    val batchScoreChanges: List[Map[TokenId, BigDecimal]] =
      trades.groupBy(_.blockNum).toList.sortBy { case (blockNum, _) => blockNum }.map {
        case (_, blockTrades) =>
          blockTrades
            .flatMap { trade =>
              for {
                entry    <- leaderboard.get(HexAddress(trade.makerAddress))
                metaInfo <- tokensMetaInfo.get(trade.tokenId)

                score =
                  trade.totalPrice / entry.avgBuy * entry.score / entry.totalLeaderboardScore * entry.totalLeaderboardSize

              } yield List(
                trade.tokenId            -> score * trade.side.sign,
                metaInfo.oppositeTokenId -> -score * trade.side.sign
              )
            }
            .flatten
            .groupMapReduce { case (tokenId, _) => tokenId } { case (_, score) => score }(_ + _)
      }

    recentScoreChangesR.update { allChanges =>
      val toDrop =
        Math.min(0, allChanges.length + batchScoreChanges.length - recentScoreChangesBlocksLength)
      allChanges.appendedAll(batchScoreChanges).drop(toDrop)
    }
  }

  private def updateRegistry(
    trades: List[Trade],
    tokensMetaInfo: Map[TokenId, TokenMetaInfo],
    leaderboard: Map[HexAddress, AdvancedLeaderboardEntry],
  ): F[List[TokenInfo]] =
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

            val tokenInfo = registry.get(trade.tokenId)

            val updatedTokenScore = tokenInfo.fold(BigDecimal(0))(_.score) + scoreFromLeader
            val updatedMaxTokenScore =
              tokenInfo
                .map(_.maxScore)
                .map(maxScore => maxScore max updatedTokenScore)
                .getOrElse(updatedTokenScore)

            val updatedTokenInfo = TokenInfo(
              id               = trade.tokenId,
              price            = trade.singleTokenPrice,
              score            = updatedTokenScore,
              maxScore         = updatedMaxTokenScore,
              resolveDate      = metaInfo.resolveDate,
              lastUpdatedBlock = trade.blockNum,
            )

            val oppositeTokenInfo = registry.get(metaInfo.oppositeTokenId)

            val updatedOppositeTokenScore = oppositeTokenInfo.fold(BigDecimal(0))(_.score) - scoreFromLeader
            val updatedMaxOppositeTokenScore =
              oppositeTokenInfo
                .map(_.maxScore)
                .map(maxScore => maxScore max updatedOppositeTokenScore)
                .getOrElse(updatedOppositeTokenScore)

            val updatedOppositeTokenInfo = TokenInfo(
              id               = metaInfo.oppositeTokenId,
              price            = 1 - trade.singleTokenPrice,
              score            = updatedOppositeTokenScore,
              maxScore         = updatedMaxOppositeTokenScore,
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

  def getTokenInfo(tokenId: TokenId): F[Option[TokenInfo]] =
    registryR.get.map(registry => registry.get(tokenId))

  def topTokensInfo: F[List[EffectiveTokenInfo]] = {
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

            EffectiveTokenInfo(
              id          = tokenInfo.id,
              efficiency  = efficiency,
              price       = tokenInfo.price,
              score       = tokenInfo.score,
              maxScore    = tokenInfo.maxScore,
              resolveDate = tokenInfo.resolveDate,
            )
        }
        .toList
        .filter(tokenInfo => tokenInfo.efficiency > 0)
        .sortBy(tokenInfo => -tokenInfo.efficiency)
    }
  }

  def topRecentTokensInfo: F[List[EffectiveTokenInfo]] = for {
    now     <- Clock[F].realTimeInstant
    changes <- recentScoreChanges
    sortedChanges = changes
      .toList
      .filter { case (_, change) => change > 0 }
      .sortBy { case (_, recentScore) => -recentScore }
      .map { case (tokenId, _) => tokenId }
    infos <- sortedChanges
      .traverse { tokenId =>
        for {
          mbTokenInfo <- getTokenInfo(tokenId)
          mbEffectiveTokenInfo = mbTokenInfo.map { tokenInfo =>
            val timeToResolve =
              (tokenInfo.resolveDate.getEpochSecond - now.getEpochSecond).max(1)
            val efficiency = (1 - tokenInfo.price) * tokenInfo.score / timeToResolve
            EffectiveTokenInfo(
              id          = tokenInfo.id,
              efficiency  = efficiency,
              price       = tokenInfo.price,
              score       = tokenInfo.score,
              maxScore    = tokenInfo.maxScore,
              resolveDate = tokenInfo.resolveDate,
            )
          }
        } yield mbEffectiveTokenInfo
      }
      .map(_.flatten)
  } yield infos

  def getRecentTokenScore(tokenId: TokenId): F[BigDecimal] = for {
    recentScores <- recentScoreChanges
    res           = recentScores.toList.filter { case (id, _) => id == tokenId }.map(_._2).sum
  } yield res

  def recentScoreChangesLength: F[Int] =
    recentScoreChangesR.get.map(_.length)

  def recentScoreChanges: F[Map[TokenId, BigDecimal]] = {
    for {
      recentScoreChanges <- recentScoreChangesR.get
      summedRecentScoreChanges = recentScoreChanges
        .flatMap(_.toList)
        .groupMapReduce { case (tokenId, _) => tokenId } { case (_, score) => score }(_ + _)
    } yield summedRecentScoreChanges
  }

  private def cleanUpAction: F[Unit] =
    Clock[F].realTimeInstant.flatMap { now =>
      registryR.update { registry =>
        registry.filter {
          case (_, tokenInfo) => tokenInfo.resolveDate.plusSeconds(60L * 60L * 24L).isAfter(now)
        }
      }
    }
}

object TokensInfoRegistry {
  def withInit[F[_]: Async](
    tokensInfoRepository: TokensInfoRepository[F],
    cleanUpPeriod: FiniteDuration,
    secondsToSellBeforeResolve: Int,
    recentScoreChangesBlocksLength: Int
  ): F[TokensInfoRegistry[F]] =
    for {
      now                 <- Clock[F].realTimeInstant
      tokensInfo          <- tokensInfoRepository.select(now)
      recentScoreChangesR <- Ref.of[F, List[Map[TokenId, BigDecimal]]](List.empty)
      registryR           <- Ref.of(tokensInfo.map(info => info.id -> info).toMap)
      tokenRegistry = new TokensInfoRegistry[F](
        registryR,
        recentScoreChangesR,
        secondsToSellBeforeResolve,
        recentScoreChangesBlocksLength,
      )
      _ <- fs2.Stream.repeatEval(tokenRegistry.cleanUpAction).metered(cleanUpPeriod).compile.drain.start
    } yield tokenRegistry
}
