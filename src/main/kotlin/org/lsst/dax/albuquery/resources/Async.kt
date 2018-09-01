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

package org.lsst.dax.albuquery.resources

import com.codahale.metrics.annotation.Timed
import com.facebook.presto.sql.parser.ParsingException
import com.facebook.presto.sql.parser.ParsingOptions
import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.tree.Query
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.annotation.JsonSerialize
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.lsst.dax.albuquery.Analyzer
import org.lsst.dax.albuquery.CONFIG
import org.lsst.dax.albuquery.ColumnMetadata
import org.lsst.dax.albuquery.EXECUTOR
import org.lsst.dax.albuquery.ErrorResponse
import org.lsst.dax.albuquery.ParsedTable
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.rewrite.TableNameRewriter
import org.lsst.dax.albuquery.tasks.QueryTask
import org.lsst.dax.albuquery.vo.TableMapper
import org.slf4j.LoggerFactory
import java.io.File
import java.net.URI
import java.nio.file.Paths
import java.util.UUID
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Context
import javax.ws.rs.core.Response
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import javax.ws.rs.FormParam
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.POST
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.UriInfo
import javax.ws.rs.core.HttpHeaders

@Path("async")
class Async(val metaservDAO: MetaservDAO) {

    data class AsyncResponse(
        val queryId: String,
        val metadata: ResponseMetadata,
        // Annotation is Workaround for https://github.com/FasterXML/jackson-module-kotlin/issues/4
        @JsonSerialize(`as` = java.util.Iterator::class) val results: Iterator<List<Any?>>
    )

    data class ResponseMetadata(val columns: List<ColumnMetadata>)

    @Context
    lateinit var uri: UriInfo
    @Context
    lateinit var headers: HttpHeaders

    @POST
    fun createQuery(
        @QueryParam("QUERY") @FormParam("QUERY") queryParam: String?,
        @QueryParam("RESPONSEFORMAT") @FormParam("RESPONSEFORMAT") formatParam: String?,
        postBody: String
    ): Response {
        val query = queryParam ?: postBody
        val format = formatParam ?: ""
        LOGGER.info("Recieved query [$query]")
        var mapper: ObjectMapper? = null
        val ct = headers.getRequestHeader(HttpHeaders.ACCEPT).get(0)
        if (ct == MediaType.APPLICATION_JSON || format.contains("json"))
            mapper = ObjectMapper().registerModule(KotlinModule())
        if (mapper == null)
            mapper = TableMapper() // default is VOTable
        return createAsyncQuery(metaservDAO, uri, query, mapper, true)
    }

    @GET
    @Path("{id}")
    @Produces(MediaType.APPLICATION_JSON)
    fun getQueryId(@PathParam("id") queryId: String): Response {
        val queryTaskFuture = findOutstandingQuery(queryId)
        if (queryTaskFuture != null) {
            val queryTask = queryTaskFuture.get(1, TimeUnit.MILLISECONDS)
            val ret = queryTask.phaseInfo.toString()
            return Response.ok(ret).build()
        } else {
            return Response.status(Response.Status.NOT_FOUND).build()
        }
    }

    @GET
    @Path("{id}/parameters")
    @Produces(MediaType.APPLICATION_JSON)
    fun getQueryParams(@PathParam("id") queryId: String): Response {
        val queryTaskFuture = findOutstandingQuery(queryId)
        if (queryTaskFuture != null) {
            val queryTask = queryTaskFuture.get(1, TimeUnit.MILLISECONDS)
            val params = queryTask.phaseInfo.parameters
            val ret = "{ 'QUERY': '$params' }"
            return Response.ok(ret).build()
        } else {
            return Response.status(Response.Status.NOT_FOUND).build()
        }
    }

    @GET
    @Path("{id}/error")
    @Produces(MediaType.APPLICATION_JSON)
    fun getQueryStatus(@PathParam("id") queryId: String): Response {
        val queryTaskFuture = findOutstandingQuery(queryId)
        if (queryTaskFuture != null) {
            val queryTask = queryTaskFuture.get(1, TimeUnit.MILLISECONDS)
            val errorFile = queryTask.phaseInfo.errorFile
            if (errorFile == "")
                return Response.ok("{ 'ERROR': 'None' }").build()
            else {
                val errorFile = getResultFile(queryId, "error")
                if (errorFile.exists()) {
                    return Response.status(Response.Status.BAD_REQUEST).entity(errorFile).build()
                } else return Response.status(Response.Status.NOT_FOUND).build()
            }
        } else {
            return Response.status(Response.Status.NOT_FOUND).build()
        }
    }

    @Timed
    @GET
    @Path("{id}/results")
    @Produces(MediaType.APPLICATION_JSON)
    fun getQueryResults(@PathParam("id") queryId: String): Response {
        val queryTaskFuture = findOutstandingQuery(queryId)
        if (queryTaskFuture != null) {
            val resultUri = getResultUri(uri, queryId, true)
            val ret = "{ 'RESULT': '$resultUri' }"
            return Response.ok(ret).build()
        } else {
            return Response.status(Response.Status.NOT_FOUND).build()
        }
    }

    @Timed
    @GET
    @Path("{id}/results/result")
    @Produces( MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON )
    fun getQueryResult(
        @PathParam("id") queryId: String,
        @QueryParam("RESPONSEFORMAT") formatParam: String?
    ): Response {
        val format = formatParam ?: ""
        val queryTaskFuture = findOutstandingQuery(queryId)
        if (queryTaskFuture != null && !queryTaskFuture.isDone()) {
            // Block until completion
            queryTaskFuture.get()
        } else return Response.status(Response.Status.NOT_FOUND).build()
        val resultFile = getResultFile(queryId, "result")
        var mt: String = MediaType.APPLICATION_XML // default
        if (resultFile.exists()) {
            if (format.contains("json"))
                mt = MediaType.APPLICATION_JSON
            else {
                val ct = headers.getRequestHeader(HttpHeaders.ACCEPT).get(0)
                if (ct.contains("json"))
                    mt = MediaType.APPLICATION_JSON
            }
            return Response.ok(resultFile, mt).build()
        }
        val errorFile = getResultFile(queryId, "error")
        if (errorFile.exists()) {
            return Response.status(Response.Status.BAD_REQUEST).entity(errorFile).build()
        }
        return Response.status(Response.Status.NOT_FOUND).build()
    }

    private fun findOutstandingQuery(queryId: String): Future<QueryTask>? {
        return OUTSTANDING_QUERY_DATABASE[queryId]
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(Async::class.java)
        val OUTSTANDING_QUERY_DATABASE = ConcurrentHashMap<String, Future<QueryTask>>()

        @Timed
        fun createAsyncQuery(
            metaservDAO: MetaservDAO,
            uri: UriInfo,
            query: String,
            objectMapper: ObjectMapper,
            resultRedirect: Boolean
        ): Response {
            val dbUri: URI
            val queryStatement: Query
            val qualifiedTables: List<ParsedTable>
            try {
                val statement = SqlParser().createStatement(query,
                    ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE)
                )
                if (statement !is Query) {
                    val err = ErrorResponse("Only Select Queries allowed", "NotSelectStatementException", null, null)
                    return Response.status(Response.Status.BAD_REQUEST).entity(err).build()
                }
                val analyzer = Analyzer.TableAndColumnExtractor()
                statement.accept(analyzer, null)
                qualifiedTables = analyzer.tables
                dbUri = Analyzer.getDatabaseURI(metaservDAO, analyzer.tables)
                // Once we've found the database URI, rewrite the query
                queryStatement = stripInstanceIdentifiers(statement)
            } catch (ex: ParsingException) {
                val err = ErrorResponse(ex.errorMessage, ex.javaClass.simpleName, null, cause = ex.message)
                return Response.status(Response.Status.BAD_REQUEST).entity(err).build()
            }

            // FIXME: Assert firstTable is fully qualified to a known database
            val queryId = UUID.randomUUID().toString()

            // FIXME: Switch statement to support different types of tasks (e.g. MySQL, Qserv-specific)
            val queryTask = QueryTask(metaservDAO, dbUri, queryId, queryStatement, qualifiedTables, objectMapper)

            // FIXME: We're reasonably certain this will execute, execute a history task
            val queryTaskFuture = EXECUTOR.submit(queryTask)

            // housekeeping
            queryTask.phaseInfo.parameters = query
            queryTask.phaseInfo.phase = "EXECUTING"

            if (objectMapper is TableMapper)
                queryTask.phaseInfo.format = MediaType.APPLICATION_XML
            else queryTask.phaseInfo.format = MediaType.APPLICATION_JSON

            // FIXME: Use a real database (User, Monolithic?)
            OUTSTANDING_QUERY_DATABASE[queryId] = queryTaskFuture

            val createdUri = getResultUri(uri, queryId, resultRedirect)
            return Response.seeOther(createdUri).build()
        }

        private fun stripInstanceIdentifiers(query: Query): Query {
            // Rewrite query to extract database instance information
            return TableNameRewriter().process(query) as Query
        }

        private fun getResultUri(uri: UriInfo, queryId: String, resultRedirect: Boolean): URI {
            val createdUriBuilder = uri.baseUriBuilder.path(Async::class.java).path(queryId)
            val createdUri = if (resultRedirect) {
                createdUriBuilder.path("results").path("result").build()
            } else {
                createdUriBuilder.build()
            }
            return createdUri
        }

        private fun getResultFile(queryId: String, rsType: String): File {
            val queryDir = Paths.get(CONFIG?.DAX_BASE_PATH, queryId)
            val resultFile = queryDir.resolve(rsType).toFile()
            return resultFile
        }
    }
}
