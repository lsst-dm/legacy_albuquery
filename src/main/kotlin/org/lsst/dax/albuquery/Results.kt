package org.lsst.dax.albuquery

import com.facebook.presto.sql.tree.AliasedRelation
import com.facebook.presto.sql.tree.QualifiedName
import com.facebook.presto.sql.tree.Relation
import com.fasterxml.jackson.annotation.JsonIgnore
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.model.metaserv.Column
import org.lsst.dax.albuquery.model.metaserv.Table
import java.sql.*
import java.util.NoSuchElementException


data class ParsedColumn(val qualifiedName: QualifiedName,
                        val alias: String?)

data class ColumnMetadata(val name: String,
                          val datatype: String?,
                          val ucd: String?,
                          val unit: String?,
                          @JsonIgnore val jdbcType: JDBCType)

data class JdbcColumnMetadata(val name: String,
                              val tableName: String,
                              val ordinal: Int,
                              val typeName: String,
                              val schemaName: String?,
                              val catalogName: String?,
                              val nullable: Int,
                              val jdbcType: JDBCType)

fun jdbcRowMetadata(rs: ResultSet): LinkedHashMap<String, JdbcColumnMetadata> {
    val resultSetMetaData = rs.metaData
    val rowMetadata = linkedMapOf<String, JdbcColumnMetadata>()

    for (i in 1 .. resultSetMetaData.columnCount) {
        val name = resultSetMetaData.getColumnName(i)
        val columnMetadata = JdbcColumnMetadata(
                name, tableName = resultSetMetaData.getTableName(i),
                ordinal = i, typeName = resultSetMetaData.getColumnTypeName(i),
                schemaName = resultSetMetaData.getSchemaName(i),
                catalogName = resultSetMetaData.getCatalogName(i),
                nullable = resultSetMetaData.isNullable(i),
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

class LookupMetadataTask(val metaservDAO: MetaservDAO,
                         val extractedRelations: List<Relation>,
                         val extractedColumns: Map<QualifiedName, ParsedColumn>) :
        Runnable {
    var columnMetadata: Map<QualifiedName, List<Column>> = mapOf()
    val tableMetadata: ArrayList<Table> = arrayListOf()

    override fun run() {
        val (schemas, columns) = buildCache()
        columnMetadata = columns
        for ((_, tables) in schemas){
            tableMetadata.addAll(tables)
        }
    }

    private fun buildCache() : Pair<
            Map<String, List<Table>>,
            Map<QualifiedName, List<Column>>>{
        val foundSchemas = linkedMapOf<String, List<Table>>()
        // "schema.table"
        val foundColumns = hashMapOf<QualifiedName, List<Column>>()
        for (relation in extractedRelations) {
            var table : com.facebook.presto.sql.tree.Table? = null
            if (relation is com.facebook.presto.sql.tree.Table) {
                // Skip relations that aren't tables
                table = relation
            }
            if (relation is AliasedRelation && relation.relation is com.facebook.presto.sql.tree.Table){
                table = relation.relation as com.facebook.presto.sql.tree.Table
            }

            if(table == null){
                continue
            }

            val qualifiedTableName = table.name
            // FIXME: Handle unqualified table names/schema names
            val databaseName = qualifiedTableName.parts.get(0)

            val database = metaservDAO.findDatabaseByName(databaseName) ?: continue

            val schema = metaservDAO.findDefaultSchemaByDatabaseId(database.id) ?: continue

            val metaservTables = metaservDAO.findTablesBySchemaId(schema.id) ?: continue
            foundSchemas.put(schema.name, metaservTables)
            for (metaservTable in metaservTables) {
                val columns = metaservDAO.findColumnsByTableId(metaservTable.id)
                foundColumns.put(QualifiedName.of(schema.name, metaservTable.name), columns)
            }
        }
        return Pair(foundSchemas, foundColumns)
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