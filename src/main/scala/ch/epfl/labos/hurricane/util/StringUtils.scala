package ch.epfl.labos.hurricane.util

object StringUtils {

  def tokenize(str: String, token: Char): Iterator[String] = new Iterator[String] {
    var i = 0

    override def hasNext: Boolean = i < str.length

    override def next(): String = {
      val j = str.indexOf(token, i)
      if(j >= 0) {
        val ret = str.substring(i, j)
        i = j + 1
        ret
      } else {
        val ret = str.substring(i)
        i = str.length
        ret
      }
    }
  }

  /*def untokenize(iter: Iterator[String], token: Char): String = {
    val buf = new StringBuilder(Config.HurricaneConfig.BackendConfig.DataConfig.chunkSize.toInt)
    iter.foreach(str => buf.append(str + token))
    buf.result
  }*/

}
