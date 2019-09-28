package bigd

/*
 * This Kotlin source file was generated by the Gradle 'init' task.
 */

import bigd.grpc.GreeterGrpc
import bigd.grpc.GreetingReply
import bigd.grpc.GreetingRequest
import io.grpc.Server
import io.grpc.ServerBuilder
import io.grpc.stub.StreamObserver
import java.util.logging.Logger

private class NodeInfo(val id: String, val port: Int) {}

private class Greeter(val slaveNodes: ArrayList<NodeInfo>): GreeterGrpc.GreeterImplBase() {
    val logger = Logger.getLogger(Greeter::class.java.name)

    override fun greetMaster(request: GreetingRequest, responseObserver: StreamObserver<GreetingReply>) {
        slaveNodes.add(NodeInfo(request.nodeId, 50052))
        logger.info("received greeting from node ${request.nodeId}")
        logger.info("Current number of slaves are ${slaveNodes.size}")
        val reply = GreetingReply.newBuilder().setMessage("Hello " + request.nodeId).build()
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}

class Master() {
    private val server: Server
    private val slaveNodes: ArrayList<NodeInfo> = arrayListOf()

    init {
        val greeter = Greeter(slaveNodes)
        server = ServerBuilder.forPort(MASTER_PORT)
                .addService(Greeter(slaveNodes))
                .build()
                .start()
        greeter.logger.info("Server started, listening on $MASTER_PORT")

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down")
                server.shutdown()
                System.err.println("*** server shut down")
            }
        })
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }
}

fun main() {
    val util = FileUtil()
    util.readfile()
    val server = Master()
    server.blockUntilShutdown()
}
