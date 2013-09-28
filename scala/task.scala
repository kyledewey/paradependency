// Basic idea: we have a series of tasks that can be
// arranged in a dependncy graph.

// Tasks take in 0 or more inputs, and have one output.  Inputs
// are named.  Tasks are wired to each other like so:

// val task1 = new Task1(...)
// val task2 = new Task2(...)
// ...
// task1.wireTo(task2, "outputName")

import java.util.concurrent._
import scala.collection.mutable.{Set => MSet}

abstract class Task[T](val formalParams: Set[String]) {
  val actualParams = new ConcurrentHashMap[String, T]()
  private val outputTo = MSet[(Task[T], String)]()
  var taskManager: TaskManager[T] = null

  def runTask(params: Map[String, T]): T

  def wireTo(otherTask: Task[T], name: String) {
    outputTo += (otherTask -> name)
  }

  def doRun() {
    import scala.collection.JavaConverters._
    assert(formalParams.size == actualParams.size)
    val result = runTask(actualParams.asScala.toMap)
    outputTo.foreach({
      case (task, output) => task.haveInput(output, result) })
  }

  private def haveInput(name: String, value: T) {
    assert(formalParams(name))
    /*val putRes = */ actualParams.put(name, value)
    //assert(putRes eq null)
    if (actualParams.size == formalParams.size) {
      taskManager.taskReady(this)
    }
  }
}

class ResultTask[T](val resName: String) extends Task[T](Set(resName)) {
  var result: Option[T] = None

  def runTask(params: Map[String, T]): T = {
    val res = params(resName)
    result = Some(res)
    res
  }
}

class TaskManager[T]() {
  import java.util.concurrent.atomic.AtomicInteger

  private val waitForTermination = new Object()
  private val pool = Executors.newFixedThreadPool(
    Runtime.getRuntime.availableProcessors)
  private val tasksRemaining = new ConcurrentHashMap[Task[T], Boolean]()
  private val tasks = MSet[Task[T]]()
  private var started = false

  def addTasks(add: Task[T]*) {
    assert(!started)
    add.foreach(t => {
      tasksRemaining.put(t, true)
      t.taskManager = this
    })
    tasks ++= add
  }

  def taskReady(task: Task[T]) {
    assert(tasks(task)) // this is one of the tasks we coordinate
    //assert(tasksRemaining.get(task) ne null) // cannot resubmit tasks
    pool.execute(new Runnable() {
      def run() {
        task.doRun()
        waitForTermination.synchronized {
          tasksRemaining.remove(task)
          waitForTermination.notify()
        }
      }
    })
  }

  def waitUntilCompletion() {
    waitForTermination.synchronized {
      while (!tasksRemaining.isEmpty) {
        waitForTermination.wait()
      }
    }
  }

  def start() {
    started = true
    tasks.foreach(task =>
      if (task.formalParams.isEmpty) {
        taskReady(task)
      })
  }
}

object Testing {
  class ConstantTask(val constant: Int) extends Task[Int](Set()) {
    def runTask(params: Map[String, Int]): Int = constant
  }

  sealed trait Binop
  case object Add extends Binop
  case object Sub extends Binop
  case object Mul extends Binop
  case object Div extends Binop

  class BinopTask(val op: Binop) extends Task[Int](Set("first", "second")) {
    def runTask(params: Map[String, Int]): Int = {
      val a = params("first")
      val b = params("second")
      op match {
        case Add => a + b
        case Sub => a - b
        case Mul => a * b
        case Div => a / b
      }
    }
  }

  // (((3 + 4) * 7) - ((6 + 2) / 4))
  val manager = new TaskManager[Int]()
  val three = new ConstantTask(3)
  val four = new ConstantTask(4)
  val seven = new ConstantTask(7)
  val six = new ConstantTask(6)
  val two = new ConstantTask(2)
  
  val add1 = new BinopTask(Add)
  val mul = new BinopTask(Mul)
  val sub = new BinopTask(Sub)
  val add2 = new BinopTask(Add)
  val div = new BinopTask(Div)

  val result = new ResultTask[Int]("result")

  three.wireTo(add1, "first")
  four.wireTo(add1, "second")
  add1.wireTo(mul, "first")
  seven.wireTo(mul, "second")
  mul.wireTo(sub, "first")

  six.wireTo(add2, "first")
  two.wireTo(add2, "second")
  add2.wireTo(div, "first")
  four.wireTo(div, "second")
  div.wireTo(sub, "second")

  sub.wireTo(result, "result")

  manager.addTasks(three, four, seven, six, two,
                   add1, add2, mul, sub, div, result)
  manager.start()
  manager.waitUntilCompletion()
}
