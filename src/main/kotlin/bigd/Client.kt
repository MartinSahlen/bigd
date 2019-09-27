package bigd

import bigd.grpc.MapReduceReply
import bigd.grpc.MapReduceRequest
import bigd.grpc.MapReducerGrpc
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.util.concurrent.TimeUnit
import io.grpc.StatusRuntimeException
import java.util.logging.Level
import java.util.logging.Logger

class Client(host: String, port: Int) {
    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build()
    private val blockingStub: MapReducerGrpc.MapReducerBlockingStub
    private val logger = Logger.getLogger(Client::class.java.name)

    init {
        this.blockingStub = MapReducerGrpc.newBlockingStub(channel)
    }

    fun shutdown() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    fun greet(name: String) {
        logger.info("Will try to greet $name ...")
        val request = MapReduceRequest.newBuilder().setUri(name).build()
        val response: MapReduceReply
        try {
            response = blockingStub.mapReduce(request)
        } catch (e: StatusRuntimeException) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.status)
            return
        }

        logger.info("Greeting: " + response.message)
    }
}

fun main() {
    val client = Client("localhost", 50051)
    try {
        /* Access a service running on the local machine on port 50051 */
        var user = "VALDEMAR"
        client.greet(user)
    } finally {
        client.shutdown()
    }
}
