package bigd

import bigd.grpc.GreeterGrpc
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import java.util.concurrent.TimeUnit
import io.grpc.StatusRuntimeException
import bigd.grpc.HelloReply
import bigd.grpc.HelloRequest
import java.util.logging.Level
import java.util.logging.Logger

class Client(host: String, port: Int) {
    private val channel: ManagedChannel = ManagedChannelBuilder.forAddress(host, port)
            .usePlaintext()
            .build()
    private val blockingStub: GreeterGrpc.GreeterBlockingStub
    private val logger = Logger.getLogger(Client::class.java.name)

    init {
        this.blockingStub = GreeterGrpc.newBlockingStub(channel)
    }

    fun shutdown() {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    fun greet(name: String) {
        logger.info("Will try to greet $name ...")
        val request = HelloRequest.newBuilder().setName(name).build()
        val response: HelloReply
        try {
            response = blockingStub.sayHello(request)
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
