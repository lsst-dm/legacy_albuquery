package org.lsst.dax.albuquery

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

class RowStreamIterator(private val conn: Connection, query: String, resultDir: Path) : Iterator<List<Any>> {
    private var row: List<Any> = listOf()
    private var emptyRow = true
    val stmt: Statement = conn.createStatement()
    val rs: ResultSet
    val jdbcColumnMetadata: LinkedHashMap<String, JdbcColumnMetadata>
    private val sqliteConnection: Connection
    private val sqliteSql: String
    val sqliteStmt: PreparedStatement

    init {
        this.rs = stmt.executeQuery(query)
        this.jdbcColumnMetadata = jdbcRowMetadata(rs)

        val resultFilePath = resultDir.resolve("result.sqlite")
        val sqliteUrl = "jdbc:sqlite:" + resultFilePath
        val changeLog = SqliteResult.buildChangeLog(jdbcColumnMetadata.values)
        SqliteResult.initializeDatabase(changeLog, sqliteUrl)
        sqliteConnection = DriverManager.getConnection(sqliteUrl)
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
                row = makeRow(rs, jdbcColumnMetadata.values.toList())
                for ((index, value) in row.withIndex()) {
                    sqliteStmt.setObject(index + 1, value)
                }
                // FIXME: addBatch
                sqliteStmt.executeUpdate()
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

    override fun next(): List<Any> {
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
        sqliteStmt.close()
        sqliteConnection.close()
        // FIXME: Write report, close Sqlite Database
    }
}

fun makeRow(rs: ResultSet, jdbcColumnMetadata: List<JdbcColumnMetadata>): List<Any> {
    /*
    Note We are just going to make a copy of the row. It's already in memory
     */
    val row: ArrayList<Any> = arrayListOf()
    for (column in jdbcColumnMetadata) {
        // TODO: Force type conversion?
        row.add(rs.getObject(column.name))
    }
    return row
}

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

fun maybeStripName(qualifiedName: QualifiedName): QualifiedName {
    if (qualifiedName.originalParts.size == 3) {
        val parts = arrayListOf<String>()
        parts.add(qualifiedName.originalParts[1])
        parts.add(qualifiedName.originalParts[2])
        return QualifiedName.of(parts)
    }
    return qualifiedName
}

fun jdbcToLsstType(jdbcType: JDBCType): String {
    return when (jdbcType) {
        JDBCType.INTEGER -> "int"
        JDBCType.SMALLINT -> "int"
        JDBCType.TINYINT -> "int"
        JDBCType.BIGINT -> "long"
        JDBCType.FLOAT -> "float"
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
