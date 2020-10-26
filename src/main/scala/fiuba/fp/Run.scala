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

object Run extends App {
  
  val blockingExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))

  val parseDataSetRow: List[String] => Option[DataSetRow] = {
    case (id :: date :: open :: high :: low :: last :: close :: diff :: curr :: ovol :: odf :: opv :: unit :: bn :: itau :: wdiff :: nil ) =>
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


  def csvParser[F[_]]: Pipe[F, Byte, List[String]] =
      _.through(text.utf8Decode)
      .through(text.lines)
      .drop(1) // remove headers
      .map(_.split(',').toList) // separate by comma

    // Get file from https://support.spatialkey.com/spatialkey-sample-csv-data/ and convert it to UTF-8
    val parser: Stream[IO, Unit] =
      io.file
        .readAll[IO](Paths.get("../train.csv"), blockingExecutionContext, 4096)
        .through(csvParser)
        .map(parseDataSetRow) // parse each line into a valid sample
        .unNoneTerminate // terminate when done
        .evalMap(x => IO(println(x)))

    val program: IO[Unit] =
      parser.compile.drain.guarantee(IO(blockingExecutionContext.shutdown()))
    
    
      program.unsafeRunSync()
}