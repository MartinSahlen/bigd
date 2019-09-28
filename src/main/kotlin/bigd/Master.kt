package bigd

import bigd.grpc.*
import io.grpc.*
import io.grpc.stub.StreamObserver
import java.util.logging.Logger

private class NodeInfo(val id: String, val port: Int, val channel: ManagedChannel, val blockingStub: MapReducerGrpc.MapReducerBlockingStub) {}

private class Greeter(val slaveNodes: ArrayList<NodeInfo>): GreeterGrpc.GreeterImplBase() {
    val logger = Logger.getLogger(Greeter::class.java.name)

    fun deliverPort(): Int {
        val usedPorts = slaveNodes.map {nodeInfo -> nodeInfo.port }
        return SLAVE_PORTS.filter {port: Int -> !usedPorts.contains(port)  }[0]
    }

    override fun greetMaster(request: GreetingRequest, responseObserver: StreamObserver<GreetingReply>) {
        val nextPort = deliverPort()
        val channel: ManagedChannel = ManagedChannelBuilder.forAddress(MASTER_HOST, nextPort)
                .usePlaintext()
                .build()
        slaveNodes.add(NodeInfo(request.nodeId, nextPort, channel, MapReducerGrpc.newBlockingStub(channel)))
        logger.info("received greeting from node ${request.nodeId}")
        logger.info("Current number of slaves are ${slaveNodes.size}")
        val reply = GreetingReply.newBuilder().setMessage("Hello " + request.nodeId).setPort(nextPort).build()

        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }

    override fun sayGoodbye(request: GoodbyeRequest, responseObserver: StreamObserver<GoodbyeReply>) {
        val nextPort = deliverPort()
        logger.info("Received goodbye from node ${request.nodeId}")
        slaveNodes.removeIf { nodeInfo -> nodeInfo.id == request.nodeId }
        logger.info("Current number of slaves are ${slaveNodes.size}")
        val reply = GoodbyeReply.newBuilder().setMessage("Goodbye " + request.nodeId).build()
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

    private fun requestDataFromNode(node: NodeInfo): Float? {
        val request = MapReduceRequest.newBuilder().setOperation("sum").setIndex("cityBike").setKey("tripduration").build()
        val response: MapReduceReply
        try {
            response = node.blockingStub.mapReduce(request)
        } catch (e: StatusRuntimeException) {
            return null
        }
        return response.value
    }

    fun requestAllData() {
        val values = slaveNodes.map {node -> requestDataFromNode(node)}
        println("Values: $values")
    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }
}

fun main() {
    val util = FileUtil()
    util.readFile()
    val server = Master()
    server.blockUntilShutdown()
}
