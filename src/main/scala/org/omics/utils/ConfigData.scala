package org.omics.utils
import com.typesafe.config.{Config, ConfigFactory}

object ConfigData {

    def getConfig(filePath:String="application.conf"):Config = {
      val conf = ConfigFactory.load(filePath)
      conf
    }

}
