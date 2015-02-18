package com.ignition.util

import java.util.concurrent.TimeUnit
import scala.collection.JavaConversions.asScalaSet
import com.typesafe.config.{ Config, ConfigException, ConfigFactory }
import com.typesafe.config.ConfigValueType.{ BOOLEAN, LIST, NULL, NUMBER, OBJECT, STRING }
import squants.thermal.Temperature
import org.joda.time.DateTime
import scala.util.Try
import org.joda.time.Duration
import squants.electro.ElectricCurrent

/**
 * Encapsulates extensions to the standard Typesafe Config functionality.
 */
object ConfigUtils {
  lazy val entireConfig = ConfigFactory.load
  lazy val rootConfig = entireConfig.getConfig("ignition")

  def getConfig(name: String) = rootConfig.getConfig(name)

  /**
   * An extension of the standard Config. Provides options and default values.
   */
  implicit class RichConfig(val cfg: Config) extends AnyVal {

    /**
     * Parses an entry as a temperature value. The entry must be formatted as a decimal number
     * followed by a valid scale suffix: F, C, or K.
     */
    @throws(classOf[ConfigException])
    def getTemperature(key: String): Temperature = {
      val str = cfg.getString(key)
      Temperature(str) getOrElse (throw new ConfigException.BadValue(key, s"$str is not a valid temperature"))
    }

    /**
     * Parses an entry as an electric current. The entry must be formatted as a decimal number
     * followed by a valid unit suffix: A or mA.
     */
    @throws(classOf[ConfigException])
    def getElectricCurrent(key: String): ElectricCurrent = {
      val str = cfg.getString(key)
      ElectricCurrent(str) getOrElse (throw new ConfigException.BadValue(key, s"$str is not a valid current"))
    }
    
    /**
     * Parses an entry as a time value.
     */
    @throws(classOf[ConfigException])
    def getTime(key: String): DateTime = {
      val str = cfg.getString(key)
      Try(DateTime.parse(str)) getOrElse (throw new ConfigException.BadValue(key, s"$str is not a valid time"))
    }

    /**
     * Parses an entry as a time interval. It uses config.getDuration() internally and then
     * wraps the number of milliseconds into a joda Duration object.
     */
    @throws(classOf[ConfigException])
    def getTimeInterval(key: String): Duration = {
      val ms = cfg.getDuration(key, TimeUnit.MILLISECONDS)
      Duration.millis(ms)
    }

    /**
     * Converts this config to a Map[String, String].
     */
    def toStringMap = cfg.entrySet map { entry =>
      val key = entry.getKey
      val value = entry.getValue
      val strValue = value.valueType match {
        case NUMBER => cfg.getNumber(key).toString
        case BOOLEAN => cfg.getBoolean(key).toString
        case STRING => cfg.getString(key)
        case LIST => cfg.getList(key).toString
        case OBJECT => cfg.getObject(key).toString
        case NULL => null
        case _ => throw new IllegalArgumentException(s"Invalid configuration value: $value")
      }
      key -> strValue
    } toMap

    /* options */

    def getBooleanOption = getOption(cfg.getBoolean) _

    def getBytesOption = getOption(cfg.getBytes) _

    def getConfigOption = getOption(cfg.getConfig) _

    def getDoubleOption = getOption(cfg.getDouble) _

    def getDurationOption(key: String, unit: TimeUnit) =
      if (cfg.hasPath(key)) Some(cfg.getDuration(key, unit))
      else None

    def getIntOption = getOption(cfg.getInt) _

    def getLongOption = getOption(cfg.getLong) _

    def getStringOption = getOption(cfg.getString) _

    def getTemperatureOption = getOption(getTemperature) _
    
    def getElectricCurrentOption = getOption(getElectricCurrent) _

    def getTimeOption = getOption(getTime) _

    def getTimeIntervalOption = getOption(getTimeInterval) _

    private def getOption[T](func: String => T)(key: String): Option[T] =
      if (cfg.hasPath(key)) Some(func(key))
      else None

    /* defaults */

    def getBooleanWithDefault = getBooleanOption andThen withDefault

    def getBytesWithDefault = getBytesOption andThen withDefault

    def getConfigWithDefault = getConfigOption andThen withDefault

    def getDoubleWithDefault = getDoubleOption andThen withDefault

    def getDurationWithDefault(key: String, unit: TimeUnit)(default: Long) =
      getDurationOption(key, unit) getOrElse default

    def getIntWithDefault = getIntOption andThen withDefault

    def getLongWithDefault = getLongOption andThen withDefault

    def getStringWithDefault = getStringOption andThen withDefault

    def getTemperatureWithDefault = getTemperatureOption andThen withDefault
    
    def getElectricCurrentWithDefault = getElectricCurrentOption andThen withDefault

    def getTimeWithDefault = getTimeOption andThen withDefault

    def getTimeIntervalWithDefault = getTimeIntervalOption andThen withDefault

    private def withDefault[T](value: Option[T])(default: T): T = value getOrElse default
  }
}