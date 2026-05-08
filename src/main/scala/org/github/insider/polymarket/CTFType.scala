package org.github.insider.polymarket

import doobie.Meta

sealed trait CTFType

object CTFType {
  case object StandardCTF extends CTFType
  case object NegRiskCTF  extends CTFType

  implicit val meta: Meta[CTFType] =
    Meta
      .StringMeta
      .imap[CTFType] {
        case "StandardCTF" => CTFType.StandardCTF
        case "NegRiskCTF"  => CTFType.NegRiskCTF
      } {
        case StandardCTF => "StandardCTF"
        case NegRiskCTF  => "NegRiskCTF"
      }
}
