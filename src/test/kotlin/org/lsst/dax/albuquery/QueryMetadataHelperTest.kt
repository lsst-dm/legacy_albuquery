package org.lsst.dax.albuquery

import com.facebook.presto.sql.parser.ParsingOptions
import com.facebook.presto.sql.parser.SqlParser
import org.junit.Test
import org.lsst.dax.albuquery.model.metaserv.Column
import org.lsst.dax.albuquery.model.metaserv.Table
import java.sql.JDBCType
import java.util.LinkedHashMap

class QueryMetadataHelperTest {

    @Test
    fun buildAndAssociateMetadata() {
        val jdbcColumnMetadata = linkedMapOf<String, JdbcColumnMetadata>()
        jdbcColumnMetadata["foo"] = JdbcColumnMetadata("foo", null, "test", 1,
                "INTEGER", "default", "default", 1, JDBCType.DOUBLE)
        jdbcColumnMetadata["bar"] = JdbcColumnMetadata("bar", null, "test", 2,
                "INTEGER", "default", "default", 1, JDBCType.FLOAT)

        val metaservTables = arrayListOf<Table>()
        val metaservColumns = linkedMapOf<Table, List<Column>>()
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
        metaservColumns.put(metaservTables[0], columnList)
        println(metaservColumns)

        var query = "SELECT * FROM test"
        var stmt = SqlParser().createStatement(query, ParsingOptions())
        var analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        var helper = QueryMetadataHelper(analyzer)
        var metadata = helper.associateMetadata(
                jdbcColumnMetadata,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT foo, bar FROM test"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata,
                metaservColumns
        )
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")
        println(metadata)

        query = "SELECT foo a, bar b FROM test"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata,
                metaservColumns
        )
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")
        println(metadata)

        query = "SELECT test.foo a, test.bar b FROM test"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT a.foo, a.bar FROM test a"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT a.foo y, a.bar z FROM test a"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT a.bar z, a.foo y FROM test a"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "jansky")
        assert(metadata[1].unit == "ergs")

        query = "SELECT test.* FROM test"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT a.* FROM test a"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "ergs")
        assert(metadata[1].unit == "jansky")

        query = "SELECT foo a FROM test"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata.filter { it.key == "foo" } as LinkedHashMap<String, JdbcColumnMetadata>,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "ergs")

        query = "SELECT foO a FROM test"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata.filter { it.key == "foo" } as LinkedHashMap<String, JdbcColumnMetadata>,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "ergs")

        query = "SELECT foO aA FROM test"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata.filter { it.key == "foo" } as LinkedHashMap<String, JdbcColumnMetadata>,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "ergs")

        query = "SELECT xY.foO aA FROM test xY"
        stmt = SqlParser().createStatement(query, ParsingOptions())
        analyzer = Analyzer.TableAndColumnExtractor()
        analyzer.process(stmt)
        helper = QueryMetadataHelper(analyzer)
        metadata = helper.associateMetadata(
                jdbcColumnMetadata.filter { it.key == "foo" } as LinkedHashMap<String, JdbcColumnMetadata>,
                metaservColumns
        )
        println(metadata)
        assert(metadata[0].unit == "ergs")
    }
}
