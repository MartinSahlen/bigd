package bigd

import java.nio.file.Files
import java.nio.file.Paths

class FileUtil {
    fun readfile() {
        Files.lines(Paths.get(this.javaClass.classLoader.getResource("data.json").toURI())).forEach(System.out::println)
    }
}