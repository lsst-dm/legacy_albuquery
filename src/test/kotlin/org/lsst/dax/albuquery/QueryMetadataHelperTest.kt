package org.lsst.dax.albuquery

import com.facebook.presto.sql.parser.ParsingOptions
import com.facebook.presto.sql.parser.SqlParser
import org.junit.Test
import org.lsst.dax.albuquery.model.metaserv.Column
import org.lsst.dax.albuquery.model.metaserv.Table
import java.sql.JDBCType
import java.util.LinkedHashMap

class QueryMetadataHelperTest {
    val metaservTables = arrayListOf<Table>()
    var metaservColumns: Pair<Table, List<Column>>

    init {
        metaservTables.add(Table(
                1,
                1,
                "default",
                "test",
                "Test Table"
        ))
        val columnList = arrayListOf<Column>()
        columnList.add(
                Column(
                        1,
                        1,
                        "test",
                        "foo",
                        "foo column",
                        1,
                        "meta.main",
                        "ergs",
                        "double",
                        false,
                        null
                )
        )
        columnList.add(
                Column(
                        2,
                        1,
                        "test",
                        "bar",
                        "bar column",
                        2,
                        "meta.main",
                        "jansky",
                        "float",
                        false,
                        null
                )
        )
        metaservColumns = Pair(metaservTables[0], columnList)
    }

    @Test
    fun buildAndAssociateMetadata() {
        var jdbcColumnMetadata = linkedMapOf<String, JdbcColumnMetadata>()
        jdbcColumnMetadata["foo"] = JdbcColumnMetadata("foo", null, "test", 1,
                "INTEGER", "default", "default", 1, JDBCType.DOUBLE)
        jdbcColumnMetadata["bar"] = JdbcColumnMetadata("bar", null, "test", 2,
                "INTEGER", "default", "default", 1, JDBCType.FLOAT)

        println(metaservColumns)
        var query = "SELECT * FROM test"
        var metadata = getMetadata(query, jdbcColumnMetadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT foo, bar FROM test"
        metadata = getMetadata(query, jdbcColumnMetadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")
        println(metadata)

        query = "SELECT foo a, bar b FROM test"
        metadata = getMetadata(query, jdbcColumnMetadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")
        println(metadata)

        query = "SELECT test.foo a, test.bar b FROM test"
        metadata = getMetadata(query, jdbcColumnMetadata)
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT a.foo y, a.bar z FROM test a"
        metadata = getMetadata(query, jdbcColumnMetadata)
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT Foo y, bar z FROM test a"
        metadata = getMetadata(query, jdbcColumnMetadata)
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT a.bar z, a.foo y FROM test a"
        metadata = getMetadata(query, jdbcColumnMetadata)
        println(metadata)
        assert(metadata[0].unit == "jansky")
        assert(metadata[1].unit == "ergs")

        query = "SELECT test.* FROM test"
        metadata = getMetadata(query, jdbcColumnMetadata)
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT a.* FROM test a"
        metadata = getMetadata(query, jdbcColumnMetadata)
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        jdbcColumnMetadata = jdbcColumnMetadata.filter { it.key == "foo" } as LinkedHashMap<String, JdbcColumnMetadata>

        query = "SELECT foo a FROM test"
        metadata = getMetadata(query, jdbcColumnMetadata)
        println(metadata)
        assert(metadata[0].unit == "ergs")

        query = "SELECT foO a FROM test"
        metadata = getMetadata(query, jdbcColumnMetadata)
        println(metadata)
        assert(metadata[0].unit == "ergs")

        query = "SELECT foO aA FROM test"
        metadata = getMetadata(query, jdbcColumnMetadata)
        println(metadata)
        assert(metadata[0].unit == "ergs")

        query = "SELECT xY.foO aA FROM test xY"
        metadata = getMetadata(query, jdbcColumnMetadata)
        println(metadata)
        assert(metadata[0].unit == "ergs")
    }

    private fun getMetadata(
        query: String,
        jdbcColumnMetadata: LinkedHashMap<String, JdbcColumnMetadata>
    ): List<ColumnMetadata> {
        val stmt = SqlParser().createStatement(query, ParsingOptions())
        val analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        val helper = QueryMetadataHelper(analyzer)
        val parsedTableToMetaserv = linkedMapOf<ParsedTable, Pair<Table, List<Column>>>()
        parsedTableToMetaserv[analyzer.tables[0]] = metaservColumns

        return helper.associateMetadata(
                jdbcColumnMetadata,
                parsedTableToMetaserv
        )
    }
}
