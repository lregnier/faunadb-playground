import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

trait Extensions {
  implicit class ExtendedFuture[A](future: Future[A]) {
    def await(implicit duration: Duration = 5 seconds): A = Await.result(future, duration)
  }
}

object Extensions extends Extensions
