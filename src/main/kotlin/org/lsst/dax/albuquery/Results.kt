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

package org.lsst.dax.albuquery

import com.codahale.metrics.annotation.Timed
import com.facebook.presto.sql.tree.QualifiedName
import com.fasterxml.jackson.annotation.JsonIgnore
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.model.metaserv.Column
import org.lsst.dax.albuquery.model.metaserv.Table
import org.lsst.dax.albuquery.results.SqliteResult
import java.nio.file.Path
import java.sql.JDBCType
import java.sql.ResultSet
import java.sql.Statement
import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException

import java.util.NoSuchElementException

/*
* For keeping track of query phase (state).
*/
data class PhaseInfo(
    var hasBooleanLiterals: Boolean = false,
    var identifier: String = "",
    var phase: String = "",
    var parameters: String = "",
    var format: String = "",
    var errorFile: String = "",
    var result: String = ""
) {
    override fun toString(): String = "{'ID': '$identifier','PHASE': '$phase','PARAMETERS': '$parameters'}"
}

/*
* Default fetch size from the database.
* We set this to 50k.
*
* The assumptions are:
* (1) Rows will be 1kB or less
* (2) There will be 4 serialized bytes also in memory for every fetched byte
* (3) This will provide an upper limit of ~250MB/query in memory at any given time
* (4) Assuming 4GB/core, this will allow us to process ~8 queries per core without
* running out of memory.
*
* */
val RS_FETCH_SIZE = 50_000

data class ParsedColumn(
    val identifier: String,
    val qualifiedName: QualifiedName,
    val alias: String?,
    val position: Int
)

data class ParsedTable(
    val identifier: String,
    val qualifiedName: QualifiedName,
    val alias: String?,
    val position: Int
)

data class ColumnMetadata(
    val name: String,
    val datatype: String?,
    val description: String,
    val ucd: String?,
    val unit: String?,
    val tableName: String?,
    @JsonIgnore val jdbcType: JDBCType
)

data class JdbcColumnMetadata(
    val name: String,
    val label: String?,
    val tableName: String,
    val ordinal: Int,
    val typeName: String,
    val schemaName: String?,
    val catalogName: String?,
    val nullable: Int,
    val jdbcType: JDBCType
)

fun jdbcRowMetadata(rs: ResultSet): LinkedHashMap<String, JdbcColumnMetadata> {
    val resultSetMetaData = rs.metaData
    val rowMetadata = linkedMapOf<String, JdbcColumnMetadata>()

    for (i in 1..resultSetMetaData.columnCount) {
        val name = resultSetMetaData.getColumnName(i)
        val columnMetadata = JdbcColumnMetadata(
            name,
            label = resultSetMetaData.getColumnLabel(i),
            tableName = resultSetMetaData.getTableName(i),
            ordinal = i,
            typeName = resultSetMetaData.getColumnTypeName(i),
            schemaName = resultSetMetaData.getSchemaName(i),
            catalogName = resultSetMetaData.getCatalogName(i),
            nullable = resultSetMetaData.isNullable(i),
            jdbcType = JDBCType.valueOf(resultSetMetaData.getColumnType(i))
        )
        rowMetadata[name] = columnMetadata
    }
    return rowMetadata
}

@Timed
class RowStreamIterator(private val conn: Connection, query: String, resultDir: Path) : Iterator<List<Any?>> {
    private var row: List<Any?> = listOf()
    private var emptyRow = true
    val stmt: Statement = conn.createStatement()
    val rs: ResultSet
    val jdbcColumnMetadata: LinkedHashMap<String, JdbcColumnMetadata>
    val jdbcColumnMetadataList: List<JdbcColumnMetadata>
    private val sqliteConnection: Connection
    private val sqliteSql: String
    val sqliteStmt: PreparedStatement
    var rowCount = 0

    init {
        this.rs = stmt.executeQuery(query)
        this.rs.fetchSize = RS_FETCH_SIZE
        this.jdbcColumnMetadata = jdbcRowMetadata(rs)
        this.jdbcColumnMetadataList = jdbcColumnMetadata.values.toList()
        val resultFilePath = resultDir.resolve("result.sqlite")
        val sqliteUrl = "jdbc:sqlite:" + resultFilePath
        val changeLog = SqliteResult.buildChangeLog(jdbcColumnMetadata.values)
        SqliteResult.initializeDatabase(changeLog, sqliteUrl)
        sqliteConnection = DriverManager.getConnection(sqliteUrl)
        sqliteConnection.autoCommit = false
        val valList = arrayListOf<String>()
        jdbcColumnMetadata.values.forEach { valList.add("?") }
        val valString = valList.joinToString(",")
        sqliteSql = "INSERT INTO result VALUES ($valString)"
        sqliteStmt = sqliteConnection.prepareStatement(sqliteSql)
        // FIXME: createSqliteDatabase(rs, jdbcColumnMetadata)
    }

    override fun hasNext(): Boolean {
        try {
            if (emptyRow) {
                if (rs.isClosed) {
                    cleanup()
                    return false
                }
                if (!rs.next()) {
                    cleanup()
                    return false
                }
                row = makeRow(rs, jdbcColumnMetadataList)
                rowCount++
                for ((index, value) in row.withIndex()) {
                    if (value == null) {
                        sqliteStmt.setNull(index + 1, rs.metaData.getColumnType(index + 1))
                    }
                    sqliteStmt.setObject(index + 1, value)
                }
                sqliteStmt.addBatch()
                if (rowCount % RS_FETCH_SIZE == 0) {
                    sqliteStmt.executeBatch()
                }
                emptyRow = false
                return true
            }
            return true
        } catch (ex: NoSuchElementException) {
            ex.printStackTrace()
            cleanup()
            return false
        } catch (ex: SQLException) {
            ex.printStackTrace()
            cleanup()
            throw RuntimeException("Error processing results", ex)
        } catch (ex: Exception) {
            ex.printStackTrace()
            throw ex
        }
    }

    override fun next(): List<Any?> {
        if (!hasNext()) {
            throw NoSuchElementException()
        }
        val ret = row
        emptyRow = true
        return ret
    }

    fun cleanup() {
        try {
            rs.close()
        } catch (ex: SQLException) {
        }
        try {
            stmt.close()
        } catch (ex: SQLException) {
        }
        try {
            conn.close()
        } catch (ex: SQLException) {
        }
        sqliteStmt.executeBatch()
        sqliteConnection.commit()
        sqliteStmt.close()
        sqliteConnection.close()
        // FIXME: Write report, close Sqlite Database
    }
}

fun makeRow(rs: ResultSet, jdbcColumnMetadata: List<JdbcColumnMetadata>): List<Any?> {
    /*
    Note We are just going to make a copy of the row. It's already in memory
     */
    val row: ArrayList<Any?> = arrayListOf()
    for (column in jdbcColumnMetadata) {
        // TODO: Force type conversion?
        row.add(rs.getObject(column.name))
    }
    return row
}

@Timed
fun lookupMetadata(metaservDAO: MetaservDAO, qualifiedTables: List<ParsedTable>):
    Map<ParsedTable, Pair<Table, List<Column>>> {
    // "schema.table"
    val foundColumns = linkedMapOf<ParsedTable, Pair<Table, List<Column>>>()
    for (parsedTable in qualifiedTables) {
        val qualifiedTableName = parsedTable.qualifiedName
        // FIXME: Handle unqualified table names/schema names
        val databaseName = qualifiedTableName.parts.get(0)

        val database = metaservDAO.findDatabaseByName(databaseName) ?: continue

        val schema = metaservDAO.findDefaultSchemaByDatabaseId(database.id) ?: continue

        val metaservTables = metaservDAO.findTablesBySchemaId(schema.id) ?: continue
        for (table in metaservTables) {
            table.schemaName = schema.name
        }
        for (metaservTable in metaservTables) {
            if (metaservTable.name == parsedTable.identifier) {
                val columns = metaservDAO.findColumnsByTableId(metaservTable.id)
                for (column in columns) {
                    column.tableName = metaservTable.name
                }
                foundColumns.put(parsedTable, Pair(metaservTable, columns))
            }
        }
    }
    return foundColumns
}

fun jdbcToLsstType(jdbcType: JDBCType): String {
    return when (jdbcType) {
        JDBCType.INTEGER -> "int"
        JDBCType.SMALLINT -> "int"
        JDBCType.TINYINT -> "int"
        JDBCType.BIGINT -> "long"
        JDBCType.FLOAT -> "float"
        JDBCType.REAL -> "float"
        JDBCType.DOUBLE -> "double"
        JDBCType.DECIMAL -> "double" // FIXME
        JDBCType.NUMERIC -> "double" // FIXME
        JDBCType.ARRAY -> "binary"
        JDBCType.BINARY -> "binary"
        JDBCType.BIT -> "binary"
        JDBCType.BLOB -> "binary"
        JDBCType.CHAR -> "string"
        JDBCType.VARCHAR -> "string"
        JDBCType.NVARCHAR -> "string"
        JDBCType.CLOB -> "string"
        JDBCType.BOOLEAN -> "boolean"
        JDBCType.DATE -> "timestamp"
        JDBCType.TIMESTAMP -> "timestamp"
        JDBCType.TIMESTAMP_WITH_TIMEZONE -> "timestamp"
        JDBCType.TIME -> "time"
        else -> "UNKNOWN"
    }
}
