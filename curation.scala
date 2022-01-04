import com.github.music.of.the.ainur.almaren.builder.Core.Implicit;
import com.github.music.of.the.ainur.almaren.Almaren;
import org.apache.spark.sql.SaveMode
import com.modak.common.credential.{Credential}
import com.modak.common._

val almaren = Almaren("Demo")
val token = sc.getConf.get("spark.nabu.token")
val api = sc.getConf.get("spark.nabu.fireshots_url")

val args = sc.getConf.get("spark.driver.args").split("\\s+")
val credentialIdJbdc = args(0).toInt
val credentialTypeIdJbdc = args(1).toInt

val credentialResult = Credential.getCredentialData(CredentialPayload(s"$token", credentialIdJbdc, credentialTypeIdJbdc, s"$api"))

val jdbcCredentials = credentialResult.data
match
{
    case jdbcCredentials: jdbcCredentials => jdbcCredentials
    case _ => throw new Exception("Currently unavailable for other credentials Types")
}
  
almaren.builder
.sourceJdbc("jdbc:postgresql://w3.training5.modak.com/training_2021","org.postgresql.Driver","select * from mt3037.user",Some(jdbcCredentials.username), Some(jdbcCredentials.password))
.targetJdbc("jdbc:postgresql://w3.training5.modak.com/training_2021","org.postgresql.Driver","mt3037.newtable",SaveMode.Overwrite,Some(jdbcCredentials.username), Some(jdbcCredentials.password))