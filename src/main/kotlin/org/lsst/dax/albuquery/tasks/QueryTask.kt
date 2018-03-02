package org.lsst.dax.albuquery.tasks

import com.facebook.presto.sql.SqlFormatter
import com.facebook.presto.sql.tree.Query
import org.lsst.dax.albuquery.Analyzer.TableAndColumnExtractor
import org.lsst.dax.albuquery.QueryMetadataHelper
import org.lsst.dax.albuquery.RowStreamIterator
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.SERVICE_ACCOUNT_CONNECTIONS
import org.lsst.dax.albuquery.lookupMetadata
import org.lsst.dax.albuquery.resources.AsyncResponse
import org.lsst.dax.albuquery.resources.ResponseMetadata
import java.net.URI
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
class QueryTask(
    val metaservDAO: MetaservDAO,
    val dbUri: URI,
    val queryId: String,
    val queryStatement: Query,
    val columnAnalyzer: TableAndColumnExtractor
) : Callable<QueryTask> {

    var entity: AsyncResponse? = null

    override fun call(): QueryTask {
        // This might be better off if it's done asynchronously, but we need some of the information
        val metaservColumns = lookupMetadata(metaservDAO, columnAnalyzer.tables)

        // Submit for data processing
        var query = SqlFormatter.formatSql(queryStatement, Optional.empty())

        // FIXME: MySQL specific hack because we can't coax Qserv to ANSI compliance
        query = query.replace("\"", "`")

        val conn = SERVICE_ACCOUNT_CONNECTIONS.getConnection(dbUri)
        val rowIterator = RowStreamIterator(conn, query, queryId)

        val columnMetadataList = QueryMetadataHelper(columnAnalyzer)
            .associateMetadata(rowIterator.jdbcColumnMetadata, metaservColumns)

        entity = AsyncResponse(
            metadata = ResponseMetadata(columnMetadataList),
            results = rowIterator.asSequence().toList() // FIXME: Not Streaming?
        )
        return this
    }
}
