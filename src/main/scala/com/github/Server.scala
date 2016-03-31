package com.github

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicLong

import akka.actor._
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.HttpMethods._
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Tcp.{ServerBinding, IncomingConnection}
import akka.stream.scaladsl.{Flow, Tcp, Source, Sink}
import akka.util.ByteString

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random



object Server {
  val Host = "localhost"
  val HttpPort = 7000
  val TcpEchoPort = 7001
  val TcpStaticPort = 7002
  val TcpOldStaticPort = 7003

  val bodyLength = 100 * 1024 * 1024
  val body = {
    ByteString(Array.ofDim[Byte](bodyLength))
  }


  def main (args: Array[String]): Unit = {



    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()

    val serverSource = Http().bind(interface = Host, port = HttpPort)

    val requestHandler: HttpRequest => HttpResponse = {
      case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
        HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, body))
    }

    val bindingFuture: Future[Http.ServerBinding] =
      serverSource.to(Sink.foreach { connection =>
        println("New http connection from " + connection.remoteAddress)

        connection handleWithSyncHandler requestHandler
        // this is equivalent to
        // connection handleWith { Flow[HttpRequest] map requestHandler }
      }).run()


    runEchoTcp(Host, TcpEchoPort)
    runStaticTcp(Host, TcpStaticPort)
    runStaticOldTcp(Host, TcpOldStaticPort)

    Await.result(system.whenTerminated, Duration.Inf)
  }


  def runEchoTcp(host: String, port: Int)(implicit system: ActorSystem, materializer: ActorMaterializer): Unit = {
    println(s"starting echo tcp on $host:$port")

    val packetSize = 10 * 1024 * 1024

    val connections: Source[IncomingConnection, Future[ServerBinding]] =
      Tcp().bind(host, port)

    connections runForeach { connection =>
      println(s"New tcp connection from: ${connection.remoteAddress}")
      val startTime = System.currentTimeMillis()

      val received = new AtomicLong(0L)

      val echo = Flow[ByteString].map { s =>
        received.addAndGet(s.length)
        val took = System.currentTimeMillis() - startTime
        println(f"Transferred ${received.get().toDouble / 1024}%.2f KB, took $took ms, packet size: ${s.length.toDouble / 1024 }%.2f KB")
        s
      }.takeWhile(_ => received.get() < packetSize)

      connection.handleWith(echo)
    }
  }


  def runStaticTcp(host: String, port: Int)(implicit system: ActorSystem, materializer: ActorMaterializer): Unit = {
    println(s"starting static tcp on $host:$port")

    val connections: Source[IncomingConnection, Future[ServerBinding]] =
      Tcp().bind(host, port)

    connections runForeach { connection =>
      println(s"New tcp connection from: ${connection.remoteAddress}")

      val echo = Flow[ByteString].take(1).map { s =>
        println(s"received ${s.utf8String}")
        ByteString("HTTP/1.1 200 OK\n\n") ++ body
      }

      connection.handleWith(echo)
    }
  }

  def runStaticOldTcp(host: String, port: Int)(implicit system: ActorSystem): Unit = {
    import akka.io.Tcp._
    println(s"starting nack static tcp on $host:$port")

    val handler = system.actorOf(Props[PublicApiEndpoint])
    akka.io.IO(akka.io.Tcp).tell(Bind(handler, new InetSocketAddress(host, port)), handler)
  }

}


class PublicApiEndpoint extends Actor with ActorLogging {
  import akka.io.Tcp._

  override val supervisorStrategy = SupervisorStrategy.stoppingStrategy


  override def postRestart(thr: Throwable): Unit = context.stop(self)

  def receive = {
    case Bound(addr) =>
      log.error(s"PublicApiEndpoint bound to ${addr.getHostString}:${addr.getPort}")
    case CommandFailed(b: Bind) =>
      log.error(s"Bind failed, stopping PublicApiEndpoint")
      context.stop(self)
    case c @ Connected(remote, local) =>
      val connection = sender()
      val nackActor = context.actorOf(NackActor.props(connection))
      nackActor ! Register(self, keepOpenOnPeerClosed = true)
    case Received(data) =>
      sender() ! Write(ByteString(s"HTTP/1.1 200 OK\nContent-length: ${Server.body.length}\n\n") ++ Server.body)
  }
}
