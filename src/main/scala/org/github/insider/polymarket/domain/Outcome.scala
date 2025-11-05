package org.github.insider.polymarket.domain

import io.circe.Decoder

sealed trait Outcome {
  import Outcome._

  override def toString: String = this match {
    case Yes => "YES"
    case No => "NO"
    case Other(_) => "OTHER"
  }
}

object Outcome {
  final case object Yes extends Outcome

  final case object No extends Outcome

  final case class Other(value: String) extends Outcome

  implicit val circeDecoder: Decoder[Outcome] =
    Decoder[String].map {
      case "Yes" => Outcome.Yes
      case "No"  => Outcome.No
      case other => Outcome.Other(other)
    }
}
