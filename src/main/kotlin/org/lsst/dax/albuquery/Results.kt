package org.lsst.dax.albuquery

import com.facebook.presto.sql.tree.QualifiedName
import java.sql.*
import java.util.NoSuchElementException


data class ParsedColumn(val qualifiedName: QualifiedName,
                        val alias: String?)

data class ColumnMetadata(val name: String,
                          val datatype: String?,
                          val ucd: String?)

data class LookupColumnMetadata(val qualifiedName: QualifiedName,
                          val alias: String?,
                          var datatype: String?,
                          var ucd: String?)

data class JdbcColumnMetadata(val name: String,
                              val ordinal: Int,
                              val typeName: String,
                              val jdbcType: JDBCType)

fun jdbcRowMetadata(rs: ResultSet): LinkedHashMap<String, JdbcColumnMetadata> {
    val resultSetMetaData = rs.metaData
    val rowMetadata = linkedMapOf<String, JdbcColumnMetadata>()

    for (i in 1 .. resultSetMetaData.columnCount) {
        val name = resultSetMetaData.getColumnName(i)
        val columnMetadata = JdbcColumnMetadata(
                name, ordinal = i, typeName = resultSetMetaData.getColumnTypeName(i),
                jdbcType = JDBCType.valueOf(resultSetMetaData.getColumnType(i))
        )
        rowMetadata[name] = columnMetadata
    }
    return rowMetadata
}

class RowIterator(val conn: Connection, query: String) : Iterator<List<Any>> {
    private var row: List<Any> = listOf()
    private var emptyRow = true
    val stmt : Statement = conn.createStatement()
    val rs : ResultSet
    val jdbcColumnMetadata: LinkedHashMap<String, JdbcColumnMetadata>


    init {
        this.rs = stmt.executeQuery(query)
        this.jdbcColumnMetadata = jdbcRowMetadata(rs)
    }

    override fun hasNext(): Boolean {
        try {
            if (emptyRow) {
                if (rs.isClosed) {
                    cleanup()
                    return false
                }
                if(!rs.next()){
                    cleanup()
                    return false
                }
                row = makeRow(rs, jdbcColumnMetadata.values.toList())
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

    fun cleanup(){
        try { rs.close() } catch (ex: SQLException) {}
        try { stmt.close() } catch (ex: SQLException) {}
        try { conn.close() } catch (ex: SQLException) {}
    }
}

fun makeRow(rs: ResultSet, jdbcColumnMetadata: List<JdbcColumnMetadata>) : List<Any> {
    /*
    Note We are just going to make a copy of the row. It's already in memory
     */
    val row : ArrayList<Any> = arrayListOf()
    for (column in jdbcColumnMetadata){
        // TODO: Force type conversion?
        row.add(rs.getObject(column.name))
    }
    return row
}

class LookupMetadataTask(val extractor: Analyzer.TableAndColumnExtractor) :
        Runnable {
    val columnMetadata: LinkedHashMap<String, LookupColumnMetadata> = linkedMapOf()

    override fun run() {
        if(extractor.allColumns){
            //
        }
        // Do some database things with the relations and columns

        // Update the columns
    }

}

fun jdbcToLsstType(jdbcType: JDBCType) : String {
    return when (jdbcType){
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