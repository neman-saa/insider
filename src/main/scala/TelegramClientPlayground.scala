import cats.effect.{IO, IOApp}
import fs2.concurrent.Topic

object TelegramClientPlayground extends IOApp.Simple {

  import canoe.api._
  import canoe.syntax._
  import cats.effect.Async
  import fs2.Stream

  def app[F[_]: Async]: F[Unit] =
    Stream
      .resource(TelegramClient[F]("8583759455:AAGY5FsULpjKl9Pj2KB59isbRY-tXjH2og0"))
      .flatMap(implicit client => Bot.polling[F].follow(greetings))
      .compile
      .drain

  def greetings[F[_]: TelegramClient]: Scenario[F, Unit] =
    for {
      chat <- Scenario.expect(command("hi").chat)
      _    <- Scenario.eval(chat.send("Hello. What's your name?"))
      name <- Scenario.expect(text)
      _    <- Scenario.eval(chat.send(s"Nice to meet you, $name"))
      _    <- Scenario.done
    } yield ()

  def run: IO[Unit] = app[IO]
}
