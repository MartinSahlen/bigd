package bigd

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Stream

class FileUtil {

    val objectMapper = ObjectMapper()

    fun readFile(fileName: String, offset: Long, limit: Long): Stream<JsonNode> {
        return Files.lines(Paths.get(this.javaClass.classLoader.getResource(fileName).toURI()))
                .skip(offset)
                .limit(limit)
                .map { objectMapper.readTree(it) }
    }
}