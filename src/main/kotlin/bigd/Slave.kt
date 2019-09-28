package bigd

import bigd.grpc.*
import io.grpc.*
import io.grpc.stub.StreamObserver
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger

private class MapReduce: MapReducerGrpc.MapReducerImplBase() {
    val logger = Logger.getLogger(MapReduce::class.java.name)

    override fun mapReduce(request: MapReduceRequest, responseObserver: StreamObserver<MapReduceReply>) {
        logger.info("Received map reduce request for index ${request.index}")
        val dataShard = DataShard(request.index, request.offset, request.limit)
        val value = dataShard.performOperation(request.key, request.operation)
        val reply = MapReduceReply.newBuilder().setValue(value).build()
        responseObserver.onNext(reply);
        responseObserver.onCompleted();
    }
}

class Slave(private val nodeId: String, host: String, port: Int) {
    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build()
    private val blockingStub: GreeterGrpc.GreeterBlockingStub
    private val logger = Logger.getLogger(Slave::class.java.name)
    private var server: Server? = null

    init {
        this.blockingStub = GreeterGrpc.newBlockingStub(channel)
    }

    fun greet(): GreetingReply? {
        logger.info("Will try to greet master at $MASTER_HOST:$MASTER_PORT")
        val request = GreetingRequest.newBuilder().setNodeId(nodeId).build()
        val response: GreetingReply
        try {
            response = blockingStub.greetMaster(request)
        } catch (e: StatusRuntimeException) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.status)
            return null
        }
        logger.info("Master is saying: " + response.message)
        return response
    }

    fun sayGoodbye() {
        logger.info("Will inform master about shudown at $MASTER_HOST:$MASTER_PORT")
        val request = GoodbyeRequest.newBuilder().setNodeId(nodeId).build()
        try {
            blockingStub.sayGoodbye(request)
        } catch (e: StatusRuntimeException) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.status)
        }
    }

    fun startServer(port: Int) {
        val mapReduce = MapReduce()
        server = ServerBuilder.forPort(port)
                .addService(MapReduce())
                .build()
                .start()
        mapReduce.logger.info("Server started, listening on $port")

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down")
                server?.shutdown()
                sayGoodbye()
                shutdownChannel()
                System.err.println("*** server shut down")
            }
        })
    }

    fun shutdownChannel() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    fun blockUntilShutdown() {
        server?.awaitTermination()
    }
}

fun main() {
    val nodeId = UUID.randomUUID().toString()
    val slave = Slave(nodeId, MASTER_HOST, MASTER_PORT)
    val reply = slave.greet()
    if (reply == null) {
        slave.shutdownChannel()
        return
    }
    slave.startServer(reply.port)
    slave.blockUntilShutdown()
}
