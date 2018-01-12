import java.net.URL
import sbt.{AutoPlugin, ModuleID}
import com.thoughtworks.sbtApiMappings.ApiMappings
import com.thoughtworks.sbtApiMappings.ApiMappings.autoImport.apiMappingRules

object KafkaApiMappingRule extends AutoPlugin {

  override def requires = ApiMappings

  override def trigger = allRequirements

  override def projectSettings =
    apiMappingRules := kafkaRule.orElse(apiMappingRules.value)

  private def kafkaRule: PartialFunction[ModuleID, URL] = {
    case ModuleID("org.apache.kafka", "kafka-streams", "1.0.0", _, _, _, _, _, _, _, _) =>
      new URL(s"https://kafka.apache.org/10/javadoc/index.html")
  }
}
