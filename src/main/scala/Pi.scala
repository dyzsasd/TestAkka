
import akka.actor._
import akka.routing.RoundRobinRouter
import akka.util.Duration
import akka.util.duration._

sealed trait PiMessage

case object Calculate extends PiMessage

case class Work(start:Int, nrOfElements:Int) extends PiMessage

case class Result(value:Double) extends PiMessage

case class PiApproximation(pi:Double, duration:Long)

class Worker extends Actor{

  def calculatePiFor(start: Int, nrOfElements: Int): Double = {
    (start until start + nrOfElements).toList.map(x=>{
      (1-x.toInt%2d*2d).toDouble/(2f*x+1).toDouble
    }).reduce((a,b)=>a+b)
  }

  override protected def receive: Receive = {
    case Work(start, nrOfElements)=> sender ! Result(calculatePiFor(start, nrOfElements))
  }
}

class Master(nrOfWorkers:Int, nrOfMessages:Int, nrOfElements: Int, listener:ActorRef) extends Actor{
  var pi:Double = _
  var nrOfResults:Int = _
  val start:Long = System.currentTimeMillis()

  val workerRouter = context.actorOf(Props[Worker].withRouter(RoundRobinRouter(nrOfWorkers)), name = "workerRouter")

  override protected def receive: Receive = {
    case Calculate => (0 until nrOfMessages).map(_*nrOfElements).foreach(workerRouter ! Work(_,nrOfElements))
    case Result(value)=>
      pi += value
      nrOfResults +=1
      if(nrOfResults == nrOfMessages){
        listener ! PiApproximation(pi, duration = (System.currentTimeMillis() - start))
      }
      context.stop(self)
  }
}

class Listener extends Actor{
  def receive = {
    case PiApproximation(pi, duration) ⇒
      println("\n\tPi approximation: \t\t%s\n\tCalculation time: \t%s"
        .format(pi, duration))
      context.system.shutdown()
  }
}

object Pi extends App{
  calculate(nrOfWorkers = 6, nrOfElements = 10000, nrOfMessages = 10000)

  // actors and messages ...

  def calculate(nrOfWorkers: Int, nrOfElements: Int, nrOfMessages: Int) {
    // Create an Akka system
    val system = ActorSystem("PiSystem")

    // create the result listener, which will print the result and shutdown the system
    val listener = system.actorOf(Props[Listener], name = "listener")

    // create the master
    val master = system.actorOf(Props(new Master(
      nrOfWorkers, nrOfMessages, nrOfElements, listener)),
      name = "master")

    // start the calculation
    master ! Calculate

  }
}
