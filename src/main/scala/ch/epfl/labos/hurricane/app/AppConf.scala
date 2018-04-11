package ch.epfl.labos.hurricane.app

import ch.epfl.labos.hurricane.frontend.FrontendConf

object AppConf {

  def fromScallop(conf: FrontendConf): AppConf =
    AppConf(conf.file(), conf.size(), conf.textMode(), Map.empty[String, String] ++ conf.genprops, Map.empty[String, String] ++ conf.hashprops)

}

case class AppConf(file: String, size: Long, textMode: Boolean, genprops: Map[String, String], hashprops: Map[String, String])
