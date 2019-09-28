package bigd

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Stream

class FileUtil {
    fun readFile() {
        val fileName = "data.json"
        val stream = Files.lines(Paths.get(this.javaClass.classLoader.getResource(fileName).toURI()))
        val count = stream.count()
        val numNodes = 4
        val shardSize = count / numNodes;
        for (i in 0 until numNodes) {
            readFile(fileName, i * shardSize, shardSize)
        }
    }

    val objectMapper = ObjectMapper()

    fun readFile(fileName: String, offset: Long, limit: Long): Stream<JsonNode> {
        return Files
                .lines(Paths.get(this.javaClass.classLoader.getResource(fileName).toURI()))
                .skip(offset)
                .limit(limit)
                .map { objectMapper.readTree(it) }
    }
}