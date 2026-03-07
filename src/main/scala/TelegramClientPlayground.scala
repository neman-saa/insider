import cats.effect.{IO, IOApp}
import fs2.concurrent.Topic
import org.github.insider.notifications.services.TelegramNotificator
import org.github.insider.polymarket.domain.Side.Buy
import org.github.insider.polymarket.domain.Trade

import scala.concurrent.duration.DurationInt

object TelegramClientPlayground extends IOApp.Simple {

  import canoe.api._
  import canoe.syntax._
  import cats.effect.Async
  import fs2.Stream
  import cats.effect.implicits._

  def app[F[_]: Async]: F[Unit] =
    Stream
      .resource(TelegramClient[F]("8583759455:AAGY5FsULpjKl9Pj2KB59isbRY-tXjH2og0"))
      .flatMap(implicit client => Bot.polling[F].follow(greetings))
      .compile
      .drain

  def greetings[F[_]: TelegramClient: Async]: Scenario[F, Unit] =
    for {
      chat  <- Scenario.expect(command("hi").chat)
      fiber <- Scenario.eval(Stream.awakeEvery[F](5.seconds).evalMap(_ => chat.send(")")).compile.drain.start)
      _     <- Scenario.expect(command("buy"))
      _     <- Scenario.eval(fiber.cancel)
      _     <- Scenario.done
    } yield ()

  def run: IO[Unit] = for {
    importantTrades <- Topic[IO, Trade]
    _ <- fs2
      .Stream
      .awakeEvery[IO](1.seconds)
      .evalMap(_ => importantTrades.publish1(Trade("testing", "testing", Buy, 10, 10, 10, "xxxxxx", 10, None)))
      .compile
      .drain
      .start
    _ <- TelegramNotificator.of[IO](importantTrades)("8583759455:AAGY5FsULpjKl9Pj2KB59isbRY-tXjH2og0").flatMap(_.create)
  } yield ()
}
