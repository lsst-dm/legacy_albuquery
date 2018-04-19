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
import org.slf4j.LoggerFactory
import java.net.URI
import java.nio.file.Paths
import java.util.UUID
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Context
import javax.ws.rs.core.Response
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import javax.ws.rs.FormParam
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.POST
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.QueryParam
import javax.ws.rs.core.UriInfo

@Path("async")
class Async(val metaservDAO: MetaservDAO) {

    data class AsyncResponse(
        val metadata: ResponseMetadata,
        // Annotation is Workaround for https://github.com/FasterXML/jackson-module-kotlin/issues/4
        @JsonSerialize(`as` = java.util.Iterator::class) val results: Iterator<List<Any?>>
    )

    data class ResponseMetadata(val columns: List<ColumnMetadata>)

    @Context
    lateinit var uri: UriInfo

    @POST
    fun createQuery(@QueryParam("query") @FormParam("query") queryParam: String?, postBody: String): Response {
        val query = queryParam ?: postBody
        LOGGER.info("Recieved query [$query]")
        val objectMapper = ObjectMapper().registerModule(KotlinModule())
        return createAsyncQuery(metaservDAO, uri, query, objectMapper, true)
    }

    @Timed
    @GET
    @Path("{id}/results/result")
    @Produces(MediaType.APPLICATION_JSON)
    fun getQuery(@PathParam("id") queryId: String): Response {
        val queryTaskFuture = findOutstandingQuery(queryId)
        if (queryTaskFuture != null) {
            // Block until completion
            queryTaskFuture.get()
        }
        val queryDir = Paths.get(CONFIG?.DAX_BASE_PATH, queryId)
        val resultFile = queryDir.resolve("result").toFile()
        if (resultFile.exists()) {
            return Response.ok(resultFile, "application/json").build()
        }
        val errorFile = queryDir.resolve("error").toFile()
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

            // FIXME: Use a real database (User, Monolithic?)
            OUTSTANDING_QUERY_DATABASE[queryId] = queryTaskFuture

            val createdUriBuilder = uri.baseUriBuilder.path(Async::class.java).path(queryId)
            val createdUri = if (resultRedirect) {
                createdUriBuilder.path("results").path("result").build()
            } else {
                createdUriBuilder.build()
            }
            return Response.seeOther(createdUri).build()
        }

        private fun stripInstanceIdentifiers(query: Query): Query {
            // Rewrite query to extract database instance information
            return TableNameRewriter().process(query) as Query
        }
    }
}
