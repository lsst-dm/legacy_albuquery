package org.lsst.dax.albuquery.tasks

import com.facebook.presto.sql.SqlFormatter
import com.facebook.presto.sql.tree.Query
import com.fasterxml.jackson.databind.ObjectMapper
import org.lsst.dax.albuquery.Analyzer.TableAndColumnExtractor
import org.lsst.dax.albuquery.CONFIG
import org.lsst.dax.albuquery.ErrorResponse
import org.lsst.dax.albuquery.ParsedTable
import org.lsst.dax.albuquery.QueryMetadataHelper
import org.lsst.dax.albuquery.RowStreamIterator
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.SERVICE_ACCOUNT_CONNECTIONS
import org.lsst.dax.albuquery.lookupMetadata
import org.lsst.dax.albuquery.resources.Async.AsyncResponse
import org.lsst.dax.albuquery.resources.Async.ResponseMetadata
import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths
import java.sql.SQLException
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
    val qualifiedTables: List<ParsedTable>,
    val objectMapper: ObjectMapper
) : Callable<QueryTask> {

    val columnAnalyzer: TableAndColumnExtractor

    init {
        columnAnalyzer = TableAndColumnExtractor()
        queryStatement.accept(columnAnalyzer, null)
    }

    override fun call(): QueryTask {
        val resultDir = Files.createDirectory(Paths.get(CONFIG?.DAX_BASE_PATH).resolve(queryId))
        // This might be better off if it's done asynchronously, but we need some of the information
        val metaservInfo = lookupMetadata(metaservDAO, qualifiedTables)

        // Submit for data processing
        var query = SqlFormatter.formatSql(queryStatement, Optional.empty())

        // FIXME: MySQL specific hack because we can't coax Qserv to ANSI compliance
        query = query.replace("\"", "`")

        val rowIterator: RowStreamIterator
        try {
            val conn = SERVICE_ACCOUNT_CONNECTIONS.getConnection(dbUri)
            rowIterator = RowStreamIterator(conn, query, resultDir)
        } catch (ex: SQLException) {
            val error = ErrorResponse(ex.message, "SQLException", null, null)
            objectMapper.writeValue(Files.newBufferedWriter(resultDir.resolve("error")), error)
            return this
        }

        val columnMetadataList = QueryMetadataHelper(columnAnalyzer)
            .associateMetadata(rowIterator.jdbcColumnMetadata, metaservInfo)

        val entity = AsyncResponse(
            metadata = ResponseMetadata(columnMetadataList),
            results = rowIterator
        )
        // Write metadata?
        // objectMapper.writeValue(Files.newBufferedWriter(resultDir.resolve("metadata.json")), entity.metadata)

        /**
         * May want to find provider ahead of time or cycle through a list of providers
         * @see javax.ws.rs.ext.MessageBodyWriter.isWriteable
         * @see javax.ws.rs.ext.MessageBodyWriter.writeTo
         */
        objectMapper.writeValue(Files.newBufferedWriter(resultDir.resolve("result")), entity)
        return this
    }
}
