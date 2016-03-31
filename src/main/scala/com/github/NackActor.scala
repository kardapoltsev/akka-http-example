package com.github

import akka.actor._
import akka.io.Tcp._
import akka.util.ByteString
import scala.annotation.tailrec
import scala.collection.immutable.Queue



class NackActor(connection: ActorRef) extends Actor with ActorLogging {
  import NackActor._

  //  val connection: ActorRef
  context.watch(connection)

  def connectionName: String =
    self.path.elements.last

  private val AckRate = 10
  var out = new OutQueue(AckRate)
  var peerClosed = false
  var frontend: ActorRef = _

  protected var lowWatermark = Int.MaxValue
  private[this] var closeCommand: Option[CloseCommand] = None
  private[this] var isReading = true

  override def receive: Receive = writeThrough

  def common: Receive = {
    case r: Received =>
      frontend ! r

    case Terminated(c) if c == connection =>
      log.debug(s"connection terminated, stopping $connectionName")
      context.stop(self)
  }

  private[this] def writeThrough: Receive = common orElse {
    case Register(frontend, keepOpen, resumeWriting) =>
      this.frontend = frontend
      connection ! Register(self, keepOpen, resumeWriting)

    case _: Write if isClosing =>
      log.warning("Can't process more writes when closing. Dropping...")

    case w @ Write(data, NoAck(noAck)) =>
      //      if (noAck != null)
      //        log.warning(s"BackPressureHandling doesn't support custom NoAcks $noAck")
      connection ! out.enqueue(w)

    case w @ Write(data, ack) =>
      connection ! out.enqueue(w, forceAck = true)

    case Abort =>
      connection ! Abort

    case c: CloseCommand if out.queueEmpty =>
      connection ! c
      context.become(closed)

    case c: CloseCommand =>
      if (isClosing)
        log.warning(s"Ignored duplicate close request when closing. $c")
      else {
        connection ! ProbeForEndOfWriting
        closeCommand = Some(c)
      }

    case PeerClosed | _: ErrorClosed =>
      log.info(s"Connection $connectionName closed by peer")
      peerClosed = true
      if (out.queueEmpty) {
        connection ! Close
        context.become(closed)
      } else {
        connection ! ProbeForEndOfWriting
        closeCommand = Some(Close)
      }

    case Ack(idx) =>
      out.dequeue(idx) //.foreach(connection ! _)
      if (!isReading && out.queueLength < lowWatermark)
        resumeReading()

    case CommandFailed(Write(_, Ack(ack))) =>
      writeFailed(ack)

    case CommandFailed(Write(_, NoAck(seq: Int))) =>
      writeFailed(seq)

    case CommandFailed(ProbeForWriteQueueEmpty) =>
      writeFailed(out.nextSequenceNo)

    case CommandFailed(ProbeForEndOfWriting) =>
      // just our probe failed, this still means the queue is empty and we can close now
      connection ! closeCommand.get
      context.become(closed)

    case ResumeReadingNow =>
      if (!isReading)
        resumeReading()

    case CanCloseNow =>
      require(isClosing, "Received unexpected CanCloseNow when not closing")
      connection ! closeCommand.get
      context.become(closed)

    case msg =>
      log.error(s"Unexpected message in write-through state: " + msg.getClass.getSimpleName)
    //      connection ! msg
  }

  def buffering(failedSeq: Int): Receive = common orElse {
    case w: Write =>
      if (isClosing)
        log.warning("Can't process more writes when closing. Dropping...")
      else
        out.enqueue(w)

    case Abort =>
      connection ! Abort

    case c: CloseCommand =>
      if (isClosing)
        log.warning(s"Ignored duplicate close request ($c) when closing.")
      else {
        // we can resume reading now (even if we don't expect any more to come in)
        // because by definition more data read can't lead to more traffic on the
        // writing side once the writing side was closed
        if (!isReading) {
          connection ! ResumeReading
          isReading = true
        }
        closeCommand = Some(c)
      }

    case c: ConnectionClosed =>
      peerClosed = true
      log.error(s"unsupported $c in buffering state")

    case WritingResumed =>
      // TODO: we are rebuilding the complete queue here to be sure all
      // the side-effects have been applied as well
      // This could be improved by reusing the internal data structures and
      // just executing the side-effects
      log.warning("Rebuilding out queue")
      val oldQueue = out
      val oldCloseCommand = closeCommand

      out = new OutQueue(AckRate, out.headSequenceNo)
      closeCommand = None
      context.become(writeThrough, discardOld = true)
      oldQueue.queue.foreach(receive) // commandPipeline is already in writeThrough state

      // we run one special probe writing request to make sure we will ResumeReading when the queue is empty
      if (!isClosing)
        connection ! ProbeForWriteQueueEmpty
      // otherwise, if we are closing we replay the close as well
      else
        receive(oldCloseCommand.get)

    case CommandFailed(_: Write)  =>
    // Drop. This is expected.

    case Ack(seq) if seq == failedSeq - 1 =>
    // Ignore. This is expected since if the last successful write was an
    // ack'd one and the next one fails (because of the ack'd one still being in the queue)
    // the CommandFailed will be received before the Ack

    case Ack(seq) =>
      log.warning(s"Unexpected Ack($seq) in buffering mode. length: ${out.queueLength} head: ${out.headSequenceNo}")

    case msg =>
      log.error(s"Unexpected message in buffering state: " + msg.getClass.getSimpleName)
    //      connection ! msg
  }

  def closed: Receive = common orElse {
    case c @ (_: Write | _: CloseCommand) =>
      log.warning(s"Connection $connectionName is already closed, dropping command $c")
    case Ack(idx) =>
      log.error(s"Unexpected ack in closed state")
    case CanCloseNow =>
    // ignore here
    case Closed =>
    // ignore here
    case msg =>
      log.error(s"Unexpected message in closed state: " + msg.getClass.getSimpleName)
    //      connection ! msg
  }

  def resumeReading(): Unit = {
    connection ! ResumeReading
    isReading = true
  }

  def writeFailed(idx: Int): Unit = {
    out.dequeue(idx - 1) //.foreach(connection ! _)

    // go into buffering mode
    connection ! ResumeWriting
    becomeBuffering(idx)
  }

  def becomeBuffering(failedSeq: Int): Unit = {
    if (!isClosing && isReading) {
      connection ! SuspendReading
      isReading = false
    }
    context.become(buffering(failedSeq), discardOld = true)
  }

  def isClosing =
    closeCommand.isDefined
}


object NackActor {
  case class Ack(offset: Int) extends Event
  object ResumeReadingNow extends Event
  object CanCloseNow extends Event
  val ProbeForWriteQueueEmpty = Write(ByteString.empty, ResumeReadingNow)
  val ProbeForEndOfWriting = Write(ByteString.empty, CanCloseNow)

  def props(connection: ActorRef) = {
    Props(new NackActor(connection))
  }

  class OutQueue(ackRate: Int, _initialSequenceNo: Int = 0) {
    // the current number of unacked Writes
    private[this] var unacked = 0
    // our buffer of sent but unacked Writes
    private[this] var buffer = Queue.empty[Write]
    // the sequence number of the first Write in the buffer
    private[this] var firstSequenceNo = _initialSequenceNo

    private[this] var length = 0

    def enqueue(w: Write, forceAck: Boolean = false): Write = {
      val seq = nextSequenceNo // is that efficient, otherwise maintain counter
      length += 1

      val shouldAck = forceAck || unacked >= ackRate - 1

      // reset the counter whenever we Ack
      if (shouldAck) unacked = 0
      else unacked += 1

      val ack = if (shouldAck) Ack(seq) else NoAck(seq)

      val write = Write(w.data, ack)
      buffer = buffer.enqueue(write)
      write
    }
    @tailrec final def dequeue(upToSeq: Int): Option[Event] =
      if (firstSequenceNo < upToSeq) {
        firstSequenceNo += 1
        //        assert(!buffer.front.wantsAck) // as we would lose it here
        if(buffer.front.wantsAck){
          throw new Exception("buffer.frond.wantsAck")
        } // as we would lose it here
        buffer = buffer.tail
        length -= 1
        dequeue(upToSeq)
      } else if (firstSequenceNo == upToSeq) { // the last one may contain an ack to send
        firstSequenceNo += 1
        val front = buffer.front
        buffer = buffer.tail
        length -= 1

        if (front.wantsAck) Some(front.ack)
        else None
      } else if (firstSequenceNo - 1 == upToSeq) None // first one failed
      else throw new IllegalStateException(s"Shouldn't get here, $firstSequenceNo > $upToSeq")

    def queue: Queue[Write] = buffer
    def queueEmpty: Boolean = length == 0
    def queueLength: Int = length
    def headSequenceNo = firstSequenceNo
    def nextSequenceNo = firstSequenceNo + length
  }
}
