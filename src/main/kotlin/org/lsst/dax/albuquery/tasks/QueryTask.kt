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

package org.lsst.dax.albuquery.tasks

import com.facebook.presto.sql.SqlFormatter
import com.facebook.presto.sql.tree.Query
import com.facebook.presto.sql.tree.Statement
import com.fasterxml.jackson.databind.ObjectMapper
import org.lsst.dax.albuquery.Analyzer.TableAndColumnExtractor
import org.lsst.dax.albuquery.CONFIG
import org.lsst.dax.albuquery.PhaseInfo
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
import java.nio.file.Path
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
    val queryStatement: Statement,
    val qualifiedTables: List<ParsedTable>,
    val objectMapper: ObjectMapper?
) : Callable<QueryTask> {

    val columnAnalyzer: TableAndColumnExtractor
    val phaseInfo: PhaseInfo

    init {
        phaseInfo = PhaseInfo(identifier = queryId, phase = "PENDING")
        columnAnalyzer = TableAndColumnExtractor()
        queryStatement.accept(columnAnalyzer, null)
    }

    private fun replaceBooleanLiterals(query: String): String {
        var replacement = query.substringAfter("WHERE ")
        replacement = replacement.replace("true", "1").replace("false", "0")
        return query.replaceAfter("WHERE ", replacement)
    }

    override fun call(): QueryTask {
        val resultDir = Files.createDirectory(Paths.get(CONFIG?.DAX_BASE_PATH).resolve(queryId))
        // This might be better off if it's done asynchronously, but we need some of the information
        val metaservInfo = lookupMetadata(metaservDAO, qualifiedTables)

        // Submit for data processing
        var query = SqlFormatter.formatSql(queryStatement, Optional.empty())

        // FIXME: MySQL specific hack because we can't coax Qserv to ANSI compliance
        if (queryStatement is Query)
            query = query.replace("\"", "`")
        else query = query.replace("\"", "") // for SHOW COLUMNS case
        // FIXME: hack due to qserv's current parser's limitation on handling top-level groupings
        var regex = """WHERE \(`qserv_(.+)\)$""".toRegex()
        query = query.replace(regex, "WHERE `qserv_$1")
        // FIXME: Really our mysql-proxy should be able to handle Boolean: true or false
        if (phaseInfo.hasBooleanLiterals)
            query = replaceBooleanLiterals(query)
        val rowIterator: RowStreamIterator
        try {
            val conn = SERVICE_ACCOUNT_CONNECTIONS.getConnection(dbUri)
            rowIterator = RowStreamIterator(conn, query, resultDir)
        } catch (ex: SQLException) {
            val error = ErrorResponse(ex.message, "SQLException",
                ex.getSQLState(), ex.errorCode.toString())
            val errorFile = resultDir.resolve("error")
            objectMapper?.writeValue(Files.newBufferedWriter(errorFile), error)
            phaseInfo.phase = "ERROR"
            phaseInfo.errorFile = errorFile.toString()
            return this
        }

        val columnMetadataList = QueryMetadataHelper(columnAnalyzer)
            .associateMetadata(rowIterator.jdbcColumnMetadata, metaservInfo)

        val entity = AsyncResponse(
            queryId = queryId,
            metadata = ResponseMetadata(columnMetadataList),
            results = rowIterator
        )
        // Write metadata?
        // objectMapper.writeValue(Files.newBufferedWriter(resultDir.resolve("metadata.json")), entity.metadata)
        val resultPath: Path = resultDir.resolve("result")
        /**
         * May want to find provider ahead of time or cycle through a list of providers
         * @see javax.ws.rs.ext.MessageBodyWriter.isWriteable
         * @see javax.ws.rs.ext.MessageBodyWriter.writeTo
         */
        objectMapper?.writeValue(Files.newBufferedWriter(resultPath), entity)
        phaseInfo.phase = "COMPLETED"
        return this
    }
}
