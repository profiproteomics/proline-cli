package fr.proline.config

import java.nio.file.Paths
import scala.collection.Map
import scala.collection.JavaConverters._

import com.typesafe.config.Config
import com.typesafe.config.ConfigObject
import com.typesafe.scalalogging.StrictLogging

import fr.profi.util.lang.EnhancedEnum
import fr.proline.core.om.provider.{IProlinePathConverter, ProlineManagedDirectoryType}
import fr.proline.cortex.api.fs.MountPoint

import pureconfig._
import pureconfig.configurable._
import pureconfig.ConvertHelpers._
import pureconfig.generic.ProductHint

//Extends ProlineManagedDirectoryType, but has MGF key
object PwxManagedDirectoryType extends EnhancedEnum {
  //val MGF_FILES = Value //TODO: get me back when we know what to do with it
  val MZDB_FILES, RAW_FILES, RESULT_FILES = Value
}

case class MountPointConfig(
  rawFiles: ConfigObject, //Map[String, com.typesafe.config.ConfigValue],
  resultFiles: ConfigObject, //Map[String, com.typesafe.config.ConfigValue],
  mzdbFiles: ConfigObject //Map[String, com.typesafe.config.ConfigValue]
)

object MountPointConfigReader {

  //import pureconfig.generic.semiauto._
  import pureconfig.generic.auto._

  //implicit val strMapReader = genericMapReader[String, com.typesafe.config.ConfigValue](catchReadError(_.toString)) // catchReadError(_.toString)
  //implicit val reader = deriveReader[MountPointConfig]

  /* Implicitly convert from kebab case to camel case */
  implicit def hint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, SnakeCase))

  def fromConfig(config: Config) = {
    ConfigSource.fromConfig(config).load[MountPointConfig] match {
      case Right(v) => Some(v)
      case Left(_)  => None
    }
  }
}


class MountPointRegistry(prolineConfig: Config) extends StrictLogging {

  import PwxManagedDirectoryType._
  
  lazy val mountPointMapping: Map[PwxManagedDirectoryType.Value, Map[String,String]] = {
    
    val mountDirTypes = PwxManagedDirectoryType.values.toArray
    val mappingBuilder = Map.newBuilder[PwxManagedDirectoryType.Value, Map[String,String]]

    val configKey = "mount_points"
    require(prolineConfig.hasPath(configKey), s"can't find the mount_points config")

    val mpConfigOpt = MountPointConfigReader.fromConfig(prolineConfig.getConfig("mount_points"))
    require(mpConfigOpt.isDefined, s"can't read the mount_points config")

    val mpConfig = mpConfigOpt.get

    for(dirType <- mountDirTypes) {
      //val configKey = "proline.mount_points."+ dirType.toString.toLowerCase
      //require(config.hasPath(configKey), s"can't read the mount_points config for directory type $dirType")
      //val labelConfig = config.getConfig("proline.mount_points."+ dirType.toString.toLowerCase)

      val mpPathByLabel = dirType match {
        case RAW_FILES => mpConfig.rawFiles
        case RESULT_FILES => mpConfig.resultFiles
        case MZDB_FILES => mpConfig.mzdbFiles
      }

      val pathByLabel = mpPathByLabel.entrySet().asScala.toSeq.map { entry =>
        val label = entry.getKey
        val path = entry.getValue.unwrapped().toString
        logger.debug(s"Found following mount point mapping (directory type = $dirType): label=$label -> path=$path")
        label -> path
      } toMap

      mappingBuilder += dirType -> pathByLabel
    }

    mappingBuilder.result
  }
  
  lazy val allMountPoints: Array[MountPoint] = {
    (for ( (dirType, pathByLabel) <- mountPointMapping; (label, path) <- pathByLabel) yield {
      MountPoint(dirType.toString.toLowerCase,label, path)
    }) toArray
  }
  
  def getAllMountPoints(): Array[MountPoint] = allMountPoints

}

object MountPointPathConverter {
  def apply(prolineConfig: Config): MountPointPathConverter = {
    new MountPointPathConverter(new MountPointRegistry(prolineConfig))
  }
}
/**
 * Created by barthe on 23/06/2015.
 */
class MountPointPathConverter(val mountPointRegistry: MountPointRegistry) extends IProlinePathConverter with StrictLogging {

  // TODO: rename me getPwxMountPointMappingForProlineDirType
  def getMountPointValue(dirType: ProlineManagedDirectoryType.Value): Map[String, String] = {
    val pwxDirType = PwxManagedDirectoryType.withName(dirType.toString)
    mountPointRegistry.mountPointMapping(pwxDirType)
  }

  def prolinePathToAbsolutePath(prolineResourcePath: String, dirType: ProlineManagedDirectoryType.Value): String = {
    val aliasedPath = Paths.get(prolineResourcePath)
    val aliasRoot = aliasedPath.subpath(0,1)

    logger.debug("aliased path  = "+aliasedPath)

    val rootPathOpt = getMountPointValue(dirType).get(aliasRoot.toString)

    logger.debug("root path = "+rootPathOpt)

    val realPath = if (rootPathOpt.isDefined) {
      val rootPath = rootPathOpt.get
      val nameCount = aliasedPath.getNameCount
      if( nameCount <= 1 ) rootPath
      else rootPath + '/' + aliasedPath.subpath(1, nameCount)
    }
    else aliasedPath.toString

    logger.debug("real path = " + realPath)

    realPath
  }
}
