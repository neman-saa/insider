package org.github.insider.polymarket

import org.http4s.Uri

package object client {
  final val GammaApiHost = Uri.unsafeFromString("https://gamma-api.polymarket.com")
}
