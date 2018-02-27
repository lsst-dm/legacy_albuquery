package org.lsst.dax.albuquery.tasks

import com.facebook.presto.sql.SqlFormatter
import com.facebook.presto.sql.tree.QualifiedName
import com.facebook.presto.sql.tree.Query
import com.facebook.presto.sql.tree.Relation
import org.lsst.dax.albuquery.*
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.model.metaserv.Column
import org.lsst.dax.albuquery.resources.AsyncResponse
import org.lsst.dax.albuquery.resources.ResponseMetadata
import java.util.Optional
import java.util.concurrent.Callable


/**
 * A Task for generic databases.
 *
 * @property metaservDAO metaserv DAO object. Used to lookup metadata in metaserv
 * @property queryId Id of query being processed. Used to store results to disk.
 * @property queryStatement Query statement
 * to disk.
 */
class QueryTask(val metaservDAO: MetaservDAO, val dbUri: String,
                val queryId: String,
                val queryStatement: Query,
                val extractedRelations: List<Relation>,
                val extractedColumns: Map<QualifiedName, ParsedColumn>) : Callable<QueryTask> {
    var entity : AsyncResponse? = null

    override fun call() : QueryTask {

        // This might be better off if it's done asynchronously, but we need some of the information
        val (tableMetadata, columnMetadata) = lookupMetadata(metaservDAO, extractedRelations)

        // Submit for data processing
        var query = SqlFormatter.formatSql(queryStatement, Optional.empty())

        // FIXME: MySQL specific hack because we can't coax Qserv to ANSI compliance
        query = query.replace("\"", "`")

        val conn = getConnection(dbUri)
        val rowIterator = RowStreamIterator(conn, query, queryId)

        val columnMetadataList = buildMetadata(rowIterator.jdbcColumnMetadata, columnMetadata)

        entity = AsyncResponse(
                metadata = ResponseMetadata(columnMetadataList),
                results = rowIterator.asSequence().toList() // FIXME: Not Streaming?
        )
        return this
    }

    private fun buildMetadata(jdbcColumnMetadata: LinkedHashMap<String, JdbcColumnMetadata>,
                              extractedColumnMetadata: Map<QualifiedName, List<Column>>): ArrayList<ColumnMetadata> {
        val columnMetadataList : ArrayList<ColumnMetadata> = arrayListOf()
        for((name, md) in jdbcColumnMetadata){
            val schemaName = md.schemaName ?: md.catalogName
            val qualifiedName = QualifiedName.of(schemaName, md.tableName)
            val metaservColumns = extractedColumnMetadata.get(qualifiedName)?.associateBy({it.name}, {it})
            val metaservColumn = metaservColumns?.get(name)
            val columnMetadata =
                    ColumnMetadata(name,
                            datatype = metaservColumn?.datatype ?: jdbcToLsstType(md.jdbcType),
                            ucd = metaservColumn?.ucd,
                            unit = metaservColumn?.unit,
                            jdbcType = md.jdbcType)
            columnMetadataList.add(columnMetadata)
        }
        return columnMetadataList
    }
}