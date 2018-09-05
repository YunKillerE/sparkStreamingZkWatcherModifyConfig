import com.beust.jcommander.Parameter

class Args extends Serializable {

  @Parameter(names = Array("-appName"), required = false) var appName: String = "spark streaming"

  @Parameter(names = Array("-brokers"), required = true) var brokers: String = null

  @Parameter(names = Array("-groupid"), required = true) var groupid: String = null

  @Parameter(names = Array("-topic"), required = true) var topic: String = null

  @Parameter(names = Array("-zkAddress"), required = true) var zkAddress: String = null

  @Parameter(names = Array("-topicCompression"), required = false) var topicCompression: String = "snappy"

  @Parameter(names = Array("-bufferMem"), required = false) var bufferMem: String = "33554432"

  @Parameter(names = Array("-lingerMs"), required = false) var lingerMs: String = "0"

  @Parameter(names = Array("-retries"), required = false) var retries: String = "0"

  @Parameter(names = Array("-durationTime"), required = false) var durationTime: Int = 10000

  @Parameter(names = Array("-perConnection"), required = false) var perConnection: String = "1"

  @Parameter(names = Array("-batchSize"), required = false) var batchSize: String = "65536"

}
