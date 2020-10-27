package fiuba.fp
import models.DataSetRow
import cats.effect._
import cats.syntax.functor._
import fs2._
import java.nio.file.Paths
import java.time.LocalDateTime
import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext
import scala.util.Try
object Converter extends IOApp {

  val parseDataSetRow: List[String] => Option[DataSetRow] = {
    case (id :: date :: open :: high :: low :: last :: close :: diff :: curr :: ovol :: odf :: opv :: unit :: bn :: itau :: wdiff :: nil) =>
      Try(
        DataSetRow(
          id = id.trim.toInt,
          date = date,
          open = open.toDoubleOption,
          high = high.toDoubleOption,
          low = low.toDoubleOption,
          last = last.toDouble,
          close = close.toDouble,
          diff = diff.toDouble,
          curr = curr,
          OVol = ovol.toIntOption,
          Odiff = odf.toIntOption,
          OpVol = opv.toIntOption,
          unit = unit,
          dollarBN = bn.toDouble,
          dollarItau = itau.toDouble,
          wDiff = wdiff.toDouble
        )
      ).toOption
    case _ => None
  }

  val converter: Stream[IO, Unit] =
    Stream.resource(Blocker[IO]).flatMap { blocker =>
      io.file
        .readAll[IO](Paths.get("./train.csv"), blocker, 4096)
        .through(csvParser)
        .map(parseDataSetRow) // parse each line into a valid sample
        .unNoneTerminate // terminate when done
        .evalMap(x => IO(println(x)))
    }

  def csvParser[F[_]]: Pipe[F, Byte, List[String]] =
    _.through(text.utf8Decode)
      .through(text.lines)
      .drop(1) // remove headers
      .map(_.split(',').toList)
  def run(args: List[String]): IO[ExitCode] =
    converter.compile.drain.as(ExitCode.Success)
}
