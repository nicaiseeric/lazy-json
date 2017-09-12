package ca.aretex.labs.data.storage

import java.io._
import java.nio.charset.{StandardCharsets, Charset}
import java.sql.ResultSet


import com.google.gson.{GsonBuilder, Gson}
import com.google.gson.stream.JsonReader

import ca.aretex.labs.data.TicketDataUtil
import ca.aretex.labs.data.TicketClassifier

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.{SerializationUtils, StringUtils}
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat
import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.feature.Word2VecModel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import org.slf4j.{Logger, LoggerFactory}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.CompressionCodecFactory

import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame, SQLContext, SaveMode}

import scala.annotation.tailrec
import scala.collection.mutable.MutableList
import scala.reflect.ClassTag
import scala.util.Try

import collection._
import JavaConversions._

/**
 * Created by Choungmo Fofack on 1/20/17.
 */
object TicketStorageService {
  private val logger: Logger = LoggerFactory.getLogger(TicketStorageService.getClass)

  def hdfsCleaner(sqlContext: SQLContext, outputPath: String) = {
    val conf: Configuration = sqlContext.sparkContext.hadoopConfiguration
    val location: Path =  new Path(outputPath)
    val fileSystem = FileSystem.get(location.toUri(), conf)
    if(fileSystem.exists(location)){
      logger.info(s"Cleaning ${outputPath}")
      fileSystem.delete(location, true)
    }
  }
  /**
   *
   * @param sqlContext
   * @param ticketClassifier
   */
  def saveModel(sqlContext: SQLContext,  ticketClassifier: TicketClassifier) = {
    logger.info(s"modelOutputPath=${ticketClassifier.modelPath}")

    hdfsCleaner(sqlContext, ticketClassifier.modelPath + "/ticketClassifier/ticket.model")
    sqlContext.sparkContext.parallelize(Seq(ticketClassifier), 1).saveAsObjectFile(ticketClassifier.modelPath + "/ticketClassifier/ticket.model")

    logger.info(s"ticketClassifier saved!")

  }

  /**
   *
   * @param sqlContext
   * @param ticketClassifier
   * @return
   */
  def loadLastModel(sqlContext: SQLContext, ticketClassifier: TicketClassifier): TicketClassifier = {
    logger.info(s"modelInputDir=${ticketClassifier.modelPath}")

    val ticketClassifierTmp = sqlContext.sparkContext.objectFile[TicketClassifier](ticketClassifier.modelPath + "/ticketClassifier/ticket.model").first()

    // We must wait until Spark-2.0.0 to have saving functions on GBT and OvsR models
//    ticketClassifier.ticketPipelineModel = PipelineModel.load(ticketClassifier.modelPath  + s"/ticketPipelineModel/decision.model")

    ticketClassifier.ticketPipelineModel = ticketClassifierTmp.ticketPipelineModel

    ticketClassifier.ticketAccuracies = ticketClassifierTmp.ticketAccuracies

    logger.info(s"ticketClassifier loaded!")
    ticketClassifier
  }

  def readStreamingHadoopSequenceFiles(streamingContext: StreamingContext, inputPath: String): DStream[Row] = {
    val ticketDstream = streamingContext.fileStream[NullWritable, BytesWritable, SequenceFileInputFormat[NullWritable, BytesWritable]](
      directory = inputPath,
      filter = (path: Path) => {val mustSkip = path.getName().startsWith("_") || path.getName().startsWith(".") || path.getName.endsWith("_"); !mustSkip},
      newFilesOnly = true,
      conf = new Configuration()
    )
    ticketDstream.map(pair => SerializationUtils.deserialize[Row](pair._2.getBytes))
  }

  def readBatchHadoopSequenceFiles(sqlContext: SQLContext, inputPath: String): RDD[Row] = {
    sqlContext.sparkContext
      .newAPIHadoopFile[NullWritable, BytesWritable, SequenceFileInputFormat[NullWritable, BytesWritable]](
        path = inputPath,
        fClass = classOf[SequenceFileInputFormat[NullWritable, BytesWritable]],
        kClass = classOf[NullWritable],
        vClass = classOf[BytesWritable],
        conf = sqlContext.sparkContext.hadoopConfiguration)
      .map(pair => SerializationUtils.deserialize[Row](pair._2.getBytes))
  }

  def writeBatchHadoopSequenceFiles(df: DataFrame, outputPath: String): Unit = {
    hdfsCleaner(df.sqlContext, outputPath)
    df.rdd.map(row => (NullWritable.get(), new BytesWritable(SerializationUtils.serialize(row))))
      .saveAsNewAPIHadoopFile[SequenceFileOutputFormat[NullWritable, BytesWritable]](path = outputPath)
  }

  def readSparkObjectFile[T:ClassTag](sqlContext: SQLContext, inputPath: String): T = {
    sqlContext.sparkContext.objectFile[T](inputPath).first()
  }
  def writeSparkObjectFile[T:ClassTag](sqlContext: SQLContext, obj: T, outputPath: String): Unit = {
    hdfsCleaner(sqlContext, outputPath)
    sqlContext.sparkContext.parallelize(Seq(obj), 1).saveAsObjectFile(outputPath)
  }

  def saveObjectToHDFS[T:ClassTag](sqlContext: SQLContext, obj: T, inputPath: String): Unit = {
    val conf: Configuration = sqlContext.sparkContext.hadoopConfiguration
    val location : Path =  new Path(inputPath)

    val fileSystem = FileSystem.get(location.toUri(), conf)

    if(fileSystem.exists(location)) fileSystem.delete(location, true)

    val objWriter: ObjectOutputStream = new ObjectOutputStream(fileSystem.create(location, true))

    objWriter.writeObject(obj)

    objWriter.flush()
    objWriter.close()

  }

  def readObjectFromHDFS[T:ClassTag](sqlContext: SQLContext, inputPath: String): T = {
    val conf: Configuration = sqlContext.sparkContext.hadoopConfiguration
    conf.setClassLoader(this.getClass.getClassLoader)

    val location : Path =  new Path(inputPath)

    val fileSystem = FileSystem.get(location.toUri(), conf)

    val objReader: ObjectInputStream = new ObjectInputStream(fileSystem.open(location))

    val obj = objReader.readObject().asInstanceOf[T]
    objReader.close()

    obj
  }


  def readObjectsFromHDFS[T:ClassTag](sqlContext: SQLContext, inputPath: String): List[T] = {
    val conf: Configuration = sqlContext.sparkContext.hadoopConfiguration
    val location : Path =  new Path(inputPath)

    val fileSystem = FileSystem.get(location.toUri(), conf)

    val factory = new CompressionCodecFactory(conf)
    val items = fileSystem.listStatus(location)

    if (items == null)  throw new java.io.FileNotFoundException(inputPath)

    val results = new MutableList[T]()
    for (item <- items) {

      // ignoring files like _SUCCESS
      val isSkipped = item.getPath().getName().startsWith("_") || item.getPath().getName().startsWith(".")
      if (!isSkipped ) {
        val codec = factory.getCodec(item.getPath())

        // check if we have a compression codec we need to use
        val stream: InputStream = if (codec != null) codec.createInputStream(fileSystem.open(item.getPath())) else fileSystem.open(item.getPath())

        val objReader: ObjectInputStream = new ObjectInputStream(stream)
        val obj = objReader.readObject().asInstanceOf[T]
        results += (obj)
        objReader.close()
      }
    }

    results.toList
  }


  /**
   *
   * @param df : the dataframe to save
   * @param outputPath : the path where to save
   * @return save the data
   */
  def writeDataFrame(df: DataFrame, outputPath: String, format:String=TicketDataUtil.FORMAT_CSV) = {
    logger.info(s"outputPath=$outputPath")
    format match {
      case TicketDataUtil.FORMAT_PARQUET =>
        df.repartition(TicketDataUtil.NB_OUTPUT_PARTITIONS)
          .write
          .mode(SaveMode.Overwrite)
          .parquet(outputPath)
      case TicketDataUtil.FORMAT_CSV =>
        df.repartition(TicketDataUtil.NB_OUTPUT_PARTITIONS)
          .write
          .option("header", "true")
          .option("delimiter", TicketDataUtil.TAB_SEPARATOR)
          .option("quote", TicketDataUtil.EMPTY_CHAR)
          .format(TicketDataUtil.FORMAT_CSV)
          .save(outputPath)
      case _ =>
        df.repartition(TicketDataUtil.NB_OUTPUT_PARTITIONS)
          .rdd
          .saveAsTextFile(outputPath)
    }
  }

  def readDataFrame(sqlContext: SQLContext, inputPath: String, format:String=TicketDataUtil.FORMAT_CSV): DataFrame = {
    logger.info(s"inputDir=$inputPath")
    format match {
      case TicketDataUtil.FORMAT_PARQUET =>
        sqlContext.read
          .parquet(inputPath)
      case TicketDataUtil.FORMAT_JSON =>
        sqlContext.read
          .json(inputPath)
      case TicketDataUtil.FORMAT_CSV =>
        sqlContext.read.option("inferSchema", "true")
          .option("header", "true")
          .option("delimiter", TicketDataUtil.TAB_SEPARATOR)
          .format(TicketDataUtil.FORMAT_CSV)
          .load(inputPath)
      case TicketDataUtil.FORMAT_TAR =>
        import sqlContext.implicits._
        val sparkContext = sqlContext.sparkContext
        sparkContext.binaryFiles(inputPath)
          .flatMapValues(x => extractFiles(x).toOption)
          .mapValues(_.map(decode()))
          .flatMap(_._2).flatMap(_.split(TicketDataUtil.LINE_SEPARATOR))
          .toDF()

      case _ => //
        import sqlContext.implicits._
        sqlContext.sparkContext.textFile(inputPath).toDF()
    }
  }


  private def extractFiles(ps: PortableDataStream, n: Int = 1024) = Try {
    val tar = new TarArchiveInputStream(new GzipCompressorInputStream(ps.open))
    Stream.continually(Option(tar.getNextTarEntry))
      // Read until next exntry is null
      .takeWhile(_.isDefined)
      // flatten
      .flatMap(x => x)
      // Drop directories
      .filter(!_.isDirectory)
      .map(
        e => {
          Stream.continually {                // Read n bytes
          val buffer = Array.fill[Byte](n)(-1)
            val i = tar.read(buffer, 0, n)
            (i, buffer.take(i))
          }          // Take as long as we've read something
            .takeWhile(_._1 > 0)
            .map(_._2)
            .flatten.toArray
        }
      )
      .toArray
  }

  private def decode(charset: Charset = StandardCharsets.UTF_8)(bytes: Array[Byte]) =
    new String(bytes, StandardCharsets.UTF_8)

  def readLinesInRDD(sqlContext: SQLContext, filelocation: String): RDD[String] = {

    val fileContents = sqlContext.sparkContext.wholeTextFiles(filelocation).flatMap{
      filecontent => {
        val raw = filecontent._2

        val results = new MutableList[String]()
        var coderpsline = ""

        for (str <- StringUtils.split(raw, "\n")) {
          if(str.startsWith("|")) coderpsline += str.stripPrefix("|")
          else {
            results += (coderpsline)
            coderpsline = str
          }
        }

        results.toList
      }
    }
    fileContents
  }


  def readLinesInList(sqlContext: SQLContext, filelocation: String): List[String] = {
    val conf: Configuration = sqlContext.sparkContext.hadoopConfiguration
    val location : Path =  new Path(filelocation)
    //try {new Path(filelocation)} catch {case ex: URISyntaxException =>  new Path(filelocation.stripPrefix("file://").stripPrefix("hdfs://bigdata-prod"))}

    val fileSystem = FileSystem.get(location.toUri(), conf)
    val factory = new CompressionCodecFactory(conf)
    val items = fileSystem.listStatus(location)
    if (items == null)
      return List.empty[String]

    val results = new MutableList[String]()
    for (item <- items) {

      // ignoring files like _SUCCESS
      val isSkipped = item.getPath().getName().startsWith("_") || item.getPath().getName().startsWith(".")
      if (!isSkipped ) {
        val codec = factory.getCodec(item.getPath());

        // check if we have a compression codec we need to use
        val stream: InputStream = if (codec != null) {
          codec.createInputStream(fileSystem.open(item.getPath()))
        } else {
          fileSystem.open(item.getPath());
        }

        val writer = new StringWriter()
        IOUtils.copy(stream, writer, "UTF-8")
        val raw = writer.toString()
        var coderpsline = ""
        for (str <- raw.split("\n")) {
          if(str.startsWith("|")) coderpsline += str.stripPrefix("|")
          else {
            results += (coderpsline)
            coderpsline = str
          }
        }
      }
    }
    return results.toList
  }


  /**
   * This class read a large JsonArray of objects of generic class T in a lazy way
   * using Scala Stream and tail-recursive functions. This requires few memory
   */
  def readJsonArrayInStream[T:ClassTag](inputStream: InputStream, debug: Boolean=false): Stream[T] = {
    import Stream._

    try {
      val reader: JsonReader = new JsonReader(new InputStreamReader(inputStream, "UTF-8"))
      val gson: Gson = new GsonBuilder().create()

      reader.beginArray()

      /**
       * this tail-rec function builds the stream of T's objects
       * it reuses the same memory stack to reduce memory consumption
       * @param hasNext indicates if an object can be parse from the JsonArray
       * @param agg contains the actual stream of objects found
       * @return
       */
      @tailrec
      def getPersons(hasNext: Boolean, agg: Stream[T]): Stream[T] ={
        if(!hasNext) agg
        else{
          val newagg = cons(gson.fromJson(reader, implicitly[ClassTag[T]].runtimeClass), agg)
          getPersons(reader.hasNext, newagg)
        }
      }

      val persons = getPersons(reader.hasNext, Stream.empty[T]).reverse

      // we close the inputstream
      reader.close()
      persons
    }
    catch {
      case ex: Throwable =>
        if(debug)
          logger.error(s"${ex.getLocalizedMessage}")
        Stream.empty[T]
    }

  }

  /**
   * This method reads a SQLite DB it should be call once for a new DB
   */
  def readSQLiteDB[T:ClassTag](sqlContext: SQLContext, url: String, table: String)(parser: ResultSet => Option[T]): RDD[T] = {
    import java.util.{List => jList, ArrayList}
    import java.sql.{Connection, DriverManager, Statement, ResultSet}

    import collection._
    import JavaConversions._

    Class.forName("org.sqlite.JDBC")

    val c : Connection = DriverManager.getConnection(url)
    c.setAutoCommit(false)
    logger.info("Opened database successfully")

    val stmt : Statement  = c.createStatement()
    val rs : ResultSet = stmt.executeQuery(s"SELECT * FROM $table;")

    val rawdatalist: jList[T] = new ArrayList[T](32768)

    while ( rs.next() ) {
      val rawdata = parser(rs)
      if(rawdata.isDefined) rawdatalist.add(rawdata.get)
    }
    
    val rawdataRDD = sqlContext.sparkContext.parallelize(rawdatalist)
    rawdataRDD
  }

  /**
   * This method reads MaxMind static DB  ==>  it should be change by API Call
   */
  case class SubList(index: Long, list: List[Int]){
    override def equals(obj: Any): Boolean = {
      obj match {
        case that: SubList => this.list containsSlice that.list
        //case that: SubList => (this.list indexOfSlice that.list) > -1
        //case that: SubList => this.list.mkString("").indexOf(that.list.mkString("")) > -1
        case _=> false
      }
    }
  }
  def test(sc: SparkContext) = {
    val A = List( List(1,2,3), List(2, 3), List(1, 44, 3), List(100, 6, 33), List(7) )
    val B = List( List(44, 1), List(100, 33), List(2, 3), List(7, 28) )

    val aRDD = sc.parallelize(A).zipWithIndex.map(t => SubList(t._2, t._1))
    val bRDD = sc.parallelize(B).zipWithIndex.map(t => SubList(t._2, t._1))

    val aiRDD = aRDD.zipWithUniqueId() ////.map(_.swap)
    val biRDD = bRDD.zipWithUniqueId() //.map(_.swap)

    aiRDD.leftOuterJoin(biRDD)
  }


}
