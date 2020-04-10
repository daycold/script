#!/usr/bin/env kscript

@file:DependsOn("commons-io:commons-io:2.6","joda-time:joda-time:2.10.5","org.apache.httpcomponents:httpclient:4.5.5")
@file:DependsOnMaven("com.github.holgerbrandl:kscript-annotations:1.4")
@file:MavenRepository("default", "http://maven.aliyun.com/nexus/content/groups/public/")

import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.InputStreamEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.joda.time.DateTime
import org.joda.time.LocalDate
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.net.URI
import java.util.regex.Pattern
import java.util.zip.GZIPInputStream

/**
 * @author Stefan Liu
 */
main(args)

fun main(args: Array<String>) {
    val test = Test()
    test.printRatio(args[0], args[1].toInt())
}

/**
 * 定义基本的筛选条件
 */
private class StatementBuilder {
    /**
     * 总请求个数条件
     */
    private val baseStatement = QueryStatement()
        .field("data.params.event.name.keyword").`is`("network")
        .field("data.params.event.duration").exist()
        .field("data.params.user.uid").exist()
        .field("data.params.app.environment.keyword").`is`("release")
        .field("data.params.event.network.path").isOneOf(listOf("tasks", "task_url"))
        .field("data.params.event.network.path").isNotOneOf(listOf("awj_lessons", "wisdom_course", "read", "next_lesson"))

    /**
     * 失败请求条件
     */
    private fun toFailedStatement(statement: QueryStatement): QueryStatement {
        return statement.copy().field("data.params.event.network.error_response").exist()
            .field("data.params.event.network.status_code").notRange(200, 500, "")
            .field("data.params.event.network.status_code").isNotOneOf(listOf(-1009, -1005, -999, -101))
    }

    fun statement() = Builder(baseStatement)

    /**
     * 添加日期和用户限制
     */
    inner class Builder(statement: QueryStatement) {
        private val queryStatement = statement.copy()
        private var startMills: Long = 1573660800000
        private var endMills: Long = 1573833599999
        private var uids: List<String> = listOf()

        fun timeRange(startTime: DateTime, endTime: DateTime): Builder {
            startMills = startTime.millis
            endMills = endTime.millis
            return this
        }

        fun userIds(userIds: List<String>): Builder {
            uids = userIds
            return this
        }

        fun toBaseQueryString(): String {
            val entityString = Entity(toBaseQuery().toBool()).toString()
            return entityString
        }

        fun toFailedQueryString(): String {
            return Entity(toFailedStatement(toBaseQuery()).toBool()).toString()
        }

        private fun toBaseQuery(): QueryStatement {
            val statement = queryStatement.copy()
            if (uids.isNotEmpty()) {
                statement.field("data.params.user.uid").isOneOf(uids)
            }
            statement.field("data.params.event.ctimestamp").range(startMills, endMills, "epoch_millis")
            return statement
        }
    }
}

class Test {
    /**
     * @param startDate 开始日期
     * @param lastDays 持续时间
     * 如 2020-01-01 ，7 则输出2020-01-01 00:00:00.000 到 2020-01-07 23:59:59.999 的数据，打印7行每天一行
     */
    fun printRatio(startDate: String, lastDays: Int) {
        val date = LocalDate.parse(startDate)
        val statement = StatementBuilder().statement()
        statement.userIds(userIds)
        println("${"index".toFixLengthString(40)} total success failure ratio")
        repeat(lastDays) {
            val dateString = date.plusDays(it).toString("yyyy-MM-dd")
            calculate(dateString, statement)
        }
    }

    /**
     * @param date 哪一天的数据（该天的00:00:00.000到23:59:59.999)
     * @param statement 筛选条件
     */
    private fun calculate(date: String, statement: StatementBuilder.Builder) {
        statement.timeRange(DateTime.parse("${date}T00:00:00.000"), DateTime.parse("${date}T23:59:59.999"))
        statement.userIds(userIds)
        val str = getResponse(statement.toBaseQueryString())
        val matcher = pattern.matcher(str)
        val totalCount =  if (matcher.find()) matcher.group(1).toInt() else -1
        val total = totalCount.toFixLengthString(5)

        val failures = getResponse(statement.toFailedQueryString())
        val failureMatch = pattern.matcher(failures)
        val failureCount =  if (failureMatch.find()) failureMatch.group(1).toInt() else -1
        val failure = failureCount.toFixLengthString(7)

        val successCount = totalCount - failureCount
        val success = successCount.toFixLengthString(7)
        val ratio = successCount.toFloat() / totalCount
        val index = "als-7eb8641d005b55e596cc89e5-${date.replace("-", ".")}"
        println("$index  $total $success $failure $ratio")
    }

    private fun getStream(str: String): InputStream {
        return ByteArrayInputStream(str.toByteArray())
    }

    private fun getResponse(content: String): String {
        val httpClient = DefaultHttpClient()
        val uri = URI("http://als.saybot.net/elasticsearch/_msearch?rest_total_hits_as_int=true&ignore_throttled=true")
        val post = HttpPost(uri)
        post.entity = InputStreamEntity(getStream(content))
        post.addHeader("Accept", "application/json, text/plain, */*")
        post.addHeader("Origin", "http://als.saybot.net")
        post.addHeader("kbn-version", "6.7.2")
        post.addHeader(
            "User-Agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.117 Safari/537.36"
        )
        post.addHeader("content-type", "application/x-ndjson")
        post.addHeader("Referer", "http://als.saybot.net/app/kibana")
        post.addHeader("Accept-Encoding", "gzip, deflate")
        post.addHeader("Accept-Language", "zh,en-US;q=0.9,en;q=0.8,zh-CN;q=0.7,la;q=0.6")
        httpClient.execute(post).use {
            val stream = it.entity.content
            try {
                GZIPInputStream(stream).use { gz ->
                    val bout = ByteArrayOutputStream()
                    IOUtils.copy(gz, bout)
                    return String(bout.toByteArray())
                }
            } catch (e: Exception) {
                stream.use { st ->
                    val bout = ByteArrayOutputStream()
                    IOUtils.copy(st, bout)
                    val str = String(bout.toByteArray())
                    return str
                }
            }
        }
    }

    private fun Any.toFixLengthString(len: Int): String {
        val string = toString()
        return if (string.length >= len) string.substring(0, len) else {
            val builder = StringBuilder()
            val head = (len - string.length) / 2
            if (head > 0) {
                repeat(head) {
                    builder.append(" ")
                }
            }
            builder.append(string)
            repeat(len - builder.length) {
                builder.append(" ")
            }
            builder.toString()
        }
    }

    private val pattern = Pattern.compile(",\"hits\":[{]\"total\":(\\d+),\"max_score\"")

    private val userIds = listOf(11173121, 11480843, 11481370, 11501336, 11503866, 11503868, 11507372, 11620967,
        11766054, 12336105, 11266963, 11481789, 11481793, 11481795, 11481804, 11487933, 11488162, 11489390, 11505096, 11507369,
        11530267, 11506345, 11564720, 11577166, 11587210, 11626954, 11627081, 11627883, 11633488, 11633496, 11634308, 11737013,
        11824959, 12243559, 11255270, 11580969, 11587176, 11616478, 11633867, 11723493, 11726319, 11735329, 12288220, 12329060,
        12361048, 12419404, 11779821, 11788504, 11793718, 11816425, 11871405, 11871589, 11872148, 11872168, 11872270, 11872272,
        11872288, 11872330, 12433980, 11796784, 11908905, 11908906, 11908909, 11908933, 11927777, 11927843, 11959634, 12032071,
        12179241, 11938725, 11938734, 11938748, 11938750, 11938751, 11943005, 11943030, 11943130, 11943140, 11943158, 12045551,
        12223240, 11785133, 11938801, 11938802, 11938803, 11938804, 11938805, 11938812, 11942921, 11949020, 11956810, 12000899,
        12155637, 12028298, 12028299, 12028300, 12028301, 12028355, 12031569, 12033981, 12154562, 12155448, 12351856, 12397715,
        12397743, 12290890, 12355562, 12355674, 12355736, 12355766, 12356898, 12356958, 12357413, 12357566, 12357674, 12358012,
        12364098, 11481797, 11723000, 11767532, 11989509, 12363398, 12363506, 12364607, 12380552, 12380649, 6999847, 11587172,
        11719082, 11720108, 11722564, 11722944, 11935844, 12209060, 12415683, 12415871, 12418933, 12419438, 12419786, 12419936,
        12420236, 12420242, 12420946, 12421949, 12421953,12435856,12460597).map { it.toString() }

}

private class QueryStatement : Statement {
    private val must: MutableList<Statement> = mutableListOf()
    private val mustNot: MutableList<Statement> = mutableListOf()

    fun toBool(): Bool {
        return Bool(must, listOf(MatchAll()), listOf(), mustNot, null)
    }

    fun field(field: String) = FieldStatement(field)

    override fun copy(): QueryStatement {
        val statement = QueryStatement()
        must.forEach {
            statement.must.add(it.copy())
        }
        mustNot.forEach {
            statement.mustNot.add(it.copy())
        }
        return statement
    }

    inner class FieldStatement(private val fieldName: String) {
        fun exist(): QueryStatement {
            must.add(Exists(fieldName))
            return this@QueryStatement
        }

        fun notExist(): QueryStatement {
            mustNot.add(Exists(fieldName))
            return this@QueryStatement
        }

        fun `is`(query: String): QueryStatement {
            must.add(MustMatch(fieldName, query))
            return this@QueryStatement
        }

        fun isNot(query: String): QueryStatement {
            mustNot.add(MustMatch(fieldName, query))
            return this@QueryStatement
        }

        fun isOneOf(queries: List<Any>): QueryStatement {
            val matches = queries.map { ShouldMatch(fieldName, it) }
            must.add(Bool(null, null, matches, null, 1))
            return this@QueryStatement
        }

        fun isNotOneOf(queries: List<Any>): QueryStatement {
            val matches = queries.map { ShouldMatch(fieldName, it) }
            mustNot.add(Bool(null, null, matches, null, 1))
            return this@QueryStatement
        }

        fun range(min: Long, max: Long, format: String): QueryStatement {
            must.add(Range(fieldName, min, max, format))
            return this@QueryStatement
        }

        fun notRange(min: Long, max: Long, format: String): QueryStatement {
            mustNot.add(Range(fieldName, min, max, format))
            return this@QueryStatement
        }
    }
}

private class Entity(private val bool: Bool, private val size: Int = 500) {
    override fun toString(): String {
        return """
{"index":"als-7eb8641d005b55e596cc89e5-*","ignore_unavailable":true,"preference":1578910573792}
{"version":true,"size":$size,"sort":[{"data.params.event.ctimestamp":{"order":"desc","unmapped_type":"boolean"}}],"_source":{"excludes":[]},"aggs":{"2":{"date_histogram":{"field":"data.params.event.ctimestamp","interval":"30m","time_zone":"Asia/Shanghai","min_doc_count":1}}},"stored_fields":["*"],"script_fields":{},"docvalue_fields":[{"field":"@timestamp","format":"date_time"},{"field":"data.params.event.ctimestamp","format":"date_time"},{"field":"data.timestamp","format":"date_time"}],"query":$bool,"highlight":{"pre_tags":["@kibana-highlighted-field@"],"post_tags":["@/kibana-highlighted-field@"],"fields":{"*":{}},"fragment_size":2147483647},"timeout":"30000ms"}
"""
    }
}

private interface Statement {
    fun copy(): Statement
}

private data class Bool(
    val must: List<Statement>?,
    val filter: List<Statement>?,
    val should: List<Statement>?,
    val mustNot: List<Statement>?,
    val minimumShouldMatch: Int?
) : Statement {
    override fun copy(): Bool {
        return Bool(must?.toList(), filter?.toList(), should?.toList(), mustNot?.toList(), minimumShouldMatch)
    }

    override fun toString(): String {
        val builder = StringBuilder("""{"bool":{""")
        if (minimumShouldMatch != null) {
            builder.append(""""minimum_should_match":$minimumShouldMatch,""")
        }
        if (must != null) {
            builder.append(""""must":${must.toStatementString()},""")
        }
        if (filter != null) {
            builder.append(""""filter":${filter.toStatementString()},""")
        }
        if (should != null) {
            builder.append(""""should":${should.toStatementString()},""")
        }
        if (mustNot != null) {
            builder.append(""""must_not":${mustNot.toStatementString()},""")
        }
        if (builder.last() == ',') {
            builder.deleteCharAt(builder.lastIndex)
        }
        builder.append("}}")
        return builder.toString()
    }

    private fun List<Statement>.toStatementString(): String {
        if (isEmpty()) {
            return "[]"
        }
        val builder = StringBuilder("[")
        forEach { builder.append(it.toString()).append(",") }
        builder.deleteCharAt(builder.lastIndex)
        builder.append("]")
        return builder.toString()
    }
}

private data class Exists(val field: String) : Statement {
    override fun copy(): Exists {
        return Exists(field)
    }

    override fun toString(): String {
        return """{"exists":{"field":"$field"}}"""
    }
}

private data class MustMatch(val field: String, val query: Any) : Statement {
    override fun copy(): MustMatch {
        return MustMatch(field, query)
    }

    override fun toString(): String {
        val queryString = if (query is Number) query.toString() else """"$query""""
        return """{"match_phrase":{"$field":{"query":$queryString}}}"""
    }
}

private data class ShouldMatch(val field: String, val query: Any) : Statement {
    override fun copy(): ShouldMatch {
        return ShouldMatch(field, query)
    }

    override fun toString(): String {
        val queryString = if (query is Number) query.toString() else """"$query""""
        return """{"match_phrase":{"$field":$queryString}}"""
    }
}

private data class Range(val field: String, val min: Long, val max: Long, val format: String) : Statement {
    override fun copy(): Range {
        return Range(field, min, max, format)
    }

    private fun getFormatString(): String {
        return if (format.isEmpty()) {
            ""
        } else {
            ""","format":"$format""""
        }
    }

    override fun toString(): String {
        return """{"range":{"$field":{"gte":$min,"lte":$max${getFormatString()}}}}"""
    }
}

private class MatchAll : Statement {
    override fun copy(): MatchAll {
        return MatchAll()
    }

    override fun toString(): String {
        return """{"match_all":{}}"""
    }
}

