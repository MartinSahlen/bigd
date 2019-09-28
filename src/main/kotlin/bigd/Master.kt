package bigd

import bigd.grpc.*
import io.grpc.*
import io.grpc.stub.StreamObserver
import java.lang.String.format
import java.nio.file.Files
import java.nio.file.Paths
import java.util.logging.Logger

private class NodeInfo(val id: String, val port: Int, val channel: ManagedChannel, val blockingStub: MapReducerGrpc.MapReducerBlockingStub) {}

private class Greeter(val slaveNodes: ArrayList<NodeInfo>, private val master: Master): GreeterGrpc.GreeterImplBase() {
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

        master.requestAllData()
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
        val greeter = Greeter(slaveNodes, this)
        server = ServerBuilder.forPort(MASTER_PORT)
                .addService(greeter)
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

    fun requestAllData() {
        val fileName = "data.json"
        val stream = Files.lines(Paths.get(this.javaClass.classLoader.getResource(fileName).toURI()))
        val count = stream.count()
        val numNodes = slaveNodes.size
        val shardSize = count / numNodes;
        val values = slaveNodes.mapIndexed {index, node ->
            val request = MapReduceRequest
                    .newBuilder()
                    .setOperation("min")
                    .setIndex(fileName)
                    .setKey("tripduration")
                    .setLimit(shardSize)
                    .setOffset(index * shardSize)
                    .build()

            val response: MapReduceReply
            try {
                response = node.blockingStub.mapReduce(request)
                response.value
                val value = response.value
                println(format("%f", value))
            } catch (e: StatusRuntimeException) {

            }
        }

    }

    fun blockUntilShutdown() {
        server.awaitTermination()
    }
}

fun main() {
    val server = Master()
    server.blockUntilShutdown()
}
