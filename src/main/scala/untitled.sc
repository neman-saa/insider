import cats.effect.IO
import cats.effect.unsafe.implicits.global

def recIO: IO[Unit] = IO.println(5).flatMap(_ => recIO)
recIO.unsafeRunSync()