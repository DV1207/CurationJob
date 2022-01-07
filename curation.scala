import import scala.util.{Try, Success, Failure}
try
{
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit;
import com.github.music.of.the.ainur.almaren.Almaren;
import com.modak.common.credential.Credential
import org.apache.spark.sql.SaveMode
import com.modak.common._

val almaren = Almaren("Demo")
    
val args = sc.getConf.get("spark.driver.args").split("\\s+")
val token = sc.getConf.get("spark.nabu.token")
val endpoint = sc.getConf.get("spark.nabu.fireshots_url")
    
val cred_id = args(0).toInt
val cred_type = args(1).toInt
  
val CredentialResult = Credential.getCredentialData(CredentialPayload(s"$token", cred_id, cred_type, s"$endpoint"))
  
val ldap = CredentialResult.data match 
{
    case ldap: ldap => ldap
    case _ => throw new Exception("Currently unable avalible for other credentials Types")
  }
  
  val username = ldap.username
  val password = ldap.password
  
almaren.builder
.sourceJdbc("jdbc:postgresql://w3.training5.modak.com/training_2021","org.postgresql.Driver","select * from mt3037.user",Some(username), Some(password))
.targetJdbc("jdbc:postgresql://w3.training5.modak.com/training_2021","org.postgresql.Driver","mt3037.newtable",SaveMode.Overwrite,Some(username), Some(password))
}
match
{
    case Success(s)=>sys.exit(0)
    case Failure(f)=>sys.exit(1)
    throw f
}


