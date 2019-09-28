package bigd

import java.nio.file.Files
import java.nio.file.Paths

class FileUtil {
    fun readFile() {
        val fileName = "data.json"
        val stream = Files.lines(Paths.get(this.javaClass.classLoader.getResource(fileName).toURI()))
        val count = stream.count()
        val numNodes = 4
        val shardSize = count / numNodes;
        for (i in 0..numNodes - 1) {
            readFile(fileName, i * shardSize, shardSize)
        }
    }

    fun readFile(fileName: String, offset: Long, limit: Long) {
        val stream = Files.lines(Paths.get(this.javaClass.classLoader.getResource(fileName).toURI()))
        val first = stream.skip(offset).findFirst()
        if (first.isPresent) {
            println(first.get())
        }
    }
}