/* This file is part of albuquery.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

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
