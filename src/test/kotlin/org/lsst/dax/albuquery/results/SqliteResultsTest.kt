package org.lsst.dax.albuquery.results

import org.junit.Test
import org.lsst.dax.albuquery.JdbcColumnMetadata
import java.nio.file.Files
import java.sql.JDBCType

class SqliteResultsTest {

    @Test
    fun createDatabase() {
        val columns = arrayListOf<JdbcColumnMetadata>()
        columns.add(
            JdbcColumnMetadata("foo", "result", "test", 1, "integer", null, null, 1, JDBCType.BIGINT)
        )
        columns.add(
            JdbcColumnMetadata("bar", "result", "test", 2, "float", null, null, 1, JDBCType.FLOAT)
        )
        columns.add(
            JdbcColumnMetadata("baz", "result", "test", 3, "double", null, null, 1, JDBCType.DOUBLE)
        )

        val tmpdir = Files.createTempDirectory("sqlitetest")
        val resultFilePath = tmpdir.resolve("test.db")
        val sqliteUrl = "jdbc:sqlite:" + resultFilePath

        val changeLog = SqliteResult.buildChangeLog(columns)
        SqliteResult.initializeDatabase(changeLog, sqliteUrl)
    }
}
