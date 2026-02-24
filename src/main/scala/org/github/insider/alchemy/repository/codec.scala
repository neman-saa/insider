package org.github.insider.alchemy.repository

import doobie.util.Put

import java.time.LocalDateTime

object codec {
  implicit val localDateTimePut: Put[LocalDateTime] = Put[String].tcontramap(_.toString)
}
