import org.neo4j.tooling._
import org.neo4j.kernel._
import collection.JavaConverters._

def main = {
  val path = "path/to/neodb/"
  val graph=new EmbeddedGraphDatabase(path)
}
