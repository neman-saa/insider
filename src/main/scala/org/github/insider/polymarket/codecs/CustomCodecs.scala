package org.github.insider.polymarket.codecs

import io.circe.Decoder

import java.time.LocalDateTime

object CustomCodecs {
  /*
   * The Gamma API returns date-time values in the format "2021-12-04T10:33:13.541Z",
   * whereas Circe's default LocalDateTime decoders expect a string without the trailing 'Z'.
   */
  implicit val localDateTimeDecoder: Decoder[LocalDateTime] =
    Decoder[String].map(_.init).map(LocalDateTime.parse)
}
