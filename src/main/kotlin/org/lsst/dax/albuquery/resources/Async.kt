package org.lsst.dax.albuquery.resources

import com.codahale.metrics.annotation.Timed
import com.facebook.presto.sql.parser.ParsingException
import com.facebook.presto.sql.parser.ParsingOptions
import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.tree.Query
import org.lsst.dax.albuquery.Analyzer
import org.lsst.dax.albuquery.ColumnMetadata
import org.lsst.dax.albuquery.EXECUTOR
import org.lsst.dax.albuquery.ErrorResponse
import org.lsst.dax.albuquery.ParsedTable
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.rewrite.TableNameRewriter
import org.lsst.dax.albuquery.tasks.QueryTask
import java.net.URI
import java.util.UUID
import java.util.logging.Logger
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Context
import javax.ws.rs.core.Response
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import javax.ws.rs.Consumes
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.POST
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.core.UriInfo

data class AsyncResponse(
    val metadata: ResponseMetadata,
    val results: List<List<Any>>
)

data class ResponseMetadata(val columns: List<ColumnMetadata>)

val DBURI = Regex("//.*")

@Path("async")
class AsyncResource(val metaservDAO: MetaservDAO) {

    @Context
    lateinit var uri: UriInfo

    @Timed
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    fun createQuery(query: String): Response {
        return createAsyncQuery(metaservDAO, uri, query, true)
    }

    @Timed
    @GET
    @Path("{id}/results/result")
    @Produces(MediaType.APPLICATION_JSON)
    fun getQuery(@PathParam("id") queryId: String): Response {
        val queryTaskFuture = findOutstandingQuery(queryId)
        if (queryTaskFuture != null) {
            // Block until completion
            val queryTask = queryTaskFuture.get()
            return Response.ok().entity(queryTask.entity).build()
        }
        return Response.status(Response.Status.NOT_FOUND).build()
    }

    private fun findOutstandingQuery(queryId: String): Future<QueryTask>? {
        return OUTSTANDING_QUERY_DATABASE[queryId]
    }

    companion object {
        private val LOGGER = Logger.getLogger(::AsyncResource.name)
        val OUTSTANDING_QUERY_DATABASE = ConcurrentHashMap<String, Future<QueryTask>>()

        fun createAsyncQuery(
            metaservDAO: MetaservDAO,
            uri: UriInfo,
            query: String,
            resultRedirect: Boolean
        ): Response {
            val dbUri: URI
            val queryStatement: Query
            val qualifiedTables: List<ParsedTable>
            try {
                val statement = SqlParser().createStatement(query, ParsingOptions())
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
            val queryTask = QueryTask(metaservDAO, dbUri, queryId, queryStatement, qualifiedTables)

            // FIXME: We're reasonably certain this will execute, execute a history task
            val queryTaskFuture = EXECUTOR.submit(queryTask)

            // FIXME: Use a real database (User, Monolithic?)
            OUTSTANDING_QUERY_DATABASE[queryId] = queryTaskFuture

            val createdUriBuilder = uri.requestUriBuilder.path(queryId)
            val createdUri = if (resultRedirect) {
                createdUriBuilder.path("results").path("result").build()
            } else {
                createdUriBuilder.build()
            }
            return Response.seeOther(createdUri).build()
        }

        private fun stripInstanceIdentifiers(query: Query): Query {
            // Rewrite query to extract database instance information
            val rewriter = TableNameRewriter()
            return rewriter.process(query) as Query
        }
    }
}
