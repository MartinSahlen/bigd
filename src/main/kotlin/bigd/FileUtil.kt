package bigd

import java.nio.file.Files
import java.nio.file.Paths

class FileUtil {
    fun readfile() {
        val fileName = "data.json"
        val stream = Files.lines(Paths.get(this.javaClass.classLoader.getResource(fileName).toURI()))
        val count = stream.count()
        val numNodes = 4
        val shardSize = count / numNodes;
        println(shardSize)
    }
}