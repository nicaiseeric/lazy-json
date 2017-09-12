package ca.aretex.labs.data

import java.text.{SimpleDateFormat, DecimalFormatSymbols, DecimalFormat}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, WeekFields}
import java.util.{Calendar, UUID, Locale}

import org.slf4j.{Logger, LoggerFactory}
import org.apache.commons.lang3.StringUtils

/**
 * Created by Choungmo Fofack on 1/16/17.
 */
object TicketDataUtil {
  private val logger: Logger = LoggerFactory.getLogger(TicketDataUtil.getClass)

  val ticketFileProperties: String = "ticket.properties"

  val THOUSAND = 1000
  val SEED = 2000l
  val THRESHOLD = 1e-9
  val EMPTY_CHAR: String = "\0"
  val EMPTY_STRING = "empty"
  val UNKNOWN_STRING = "unknown"

  val NB_OUTPUT_PARTITIONS = 5
  val NB_SAMPLES = 5


  val LABEL_INIT = -1
  val LABEL_ZERO = 0
  val LABEL_ONE = 1
  val LABEL_TWO = 2

  val FORMAT_CSV = "com.databricks.spark.csv"
  val FORMAT_PARQUET = "parquet"
  val FORMAT_JSON = "json"
  val FORMAT_TAR ="targz"
  val FORMAT_DATE="yyyyMMdd"
  val FORMATTER_NUMBER = new DecimalFormat("#.####", new DecimalFormatSymbols(Locale.US))
  val FORMATTER_DATE = DateTimeFormatter.ofPattern(FORMAT_DATE)

  val QUOTE = "\""
  val SLASH = "/"
  val SINGLEPIPE_DELIMITER = "|"
  val DOUBLEPIPE_DELIMITER = "||"
  val SPACE_SEPARATOR = " "
  val COLON_SEPARATOR = ":"
  val TAB_SEPARATOR = "\t"
  val LINE_SEPARATOR = "\n"
  val SEMICOLON_SEPARATOR = ";"
  val COMMA_SEPARATOR = ","

  val SINGLEPIPE_REGEX = "\\|"
  val DOUBLEPIPE_REGEX = "\\|\\|"
  val NON_CAPTURING_REGEX = "?:"
  val PATH_REGEX = "([a-zA-Z]+:/)?(/?[a-zA-Z0-9_.-]+)+"

  def getDateAndInstant(): (String, Long) = {
    val date = Calendar.getInstance().getTime
    val instant = date.getTime
    val ticketAppRunDate = new SimpleDateFormat(FORMAT_DATE).format(date)
    (ticketAppRunDate, instant)
  }

  def getYearAndWeek(strdate: String): (Int, Int) = {
    val date = LocalDateTime.parse(s"${strdate}_120000", FORMATTER_DATE)
    (date.getYear, date.get(WeekFields.of(Locale.FRANCE).weekOfWeekBasedYear()))
  }

  def getYearMonthAndWeek(strdate: String): (Int, Int, Int) = {
    val date = LocalDateTime.parse(s"${strdate}_120000", FORMATTER_DATE)
    (date.getYear, date.getMonthValue, date.get(WeekFields.of(Locale.FRANCE).weekOfWeekBasedYear()))
  }

  def getFirstDateOfMonth(strdate: String): String = {
    val date = LocalDateTime.parse(s"${strdate}_120000", FORMATTER_DATE).`with`(ChronoField.DAY_OF_MONTH , 1 )
    date.toLocalDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
  }

  def cleans(line: String, repl: String=TicketDataUtil.QUOTE): String =
    StringUtils.removeEnd(StringUtils.removeStart(line, repl), repl)

  def stripQuotes(s: String, repl: String="\"", bothSide: Boolean=true): String =
    if(bothSide) s.stripPrefix(repl).stripSuffix(repl) else s.stripPrefix(repl)
  def splits: (String, String) => Array[String] = StringUtils.splitByWholeSeparatorPreserveAllTokens
  def splitsLimit: (String, String, Int) => Array[String] = StringUtils.splitByWholeSeparatorPreserveAllTokens

  def errorHandler(bogus: String, t: Throwable): Unit = logger.warn(t.getMessage + s": $bogus")

  def generateUUID(prefix: String): String = s"${prefix}_${UUID.randomUUID.toString.takeRight(12)}"
}
*/


