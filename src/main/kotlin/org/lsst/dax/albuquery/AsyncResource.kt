package org.lsst.dax.albuquery

import com.facebook.presto.sql.parser.ParsingException
import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.tree.QualifiedName
import com.facebook.presto.sql.tree.Statement
import com.facebook.presto.sql.tree.Table
import java.sql.DriverManager
import java.util.concurrent.Callable
import java.util.logging.Logger
import javax.ws.rs.core.MediaType.APPLICATION_JSON
import javax.ws.rs.core.Context
import javax.ws.rs.core.Response
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger
import javax.ws.rs.*
import javax.ws.rs.core.UriInfo

data class AsyncResponse(val metadata: ResponseMetadata,
                         val results: List<List<Any>>)

data class ResponseMetadata(val columns: List<ColumnMetadata>)

@Path("async")
class AsyncResource {

    private val LOGGER = Logger.getLogger("AsyncResource")
    @Context lateinit var uri : UriInfo
    val QUERY_DATABASE = ConcurrentHashMap<Int, Future<QueryTask>>()
    val QUERY_ID_INT = AtomicInteger(1)

    init {
        // Initialize webserver stuff
    }

    @POST
    fun createQuery(query: String): Response {
        try {
            val firstTable = getFirstTable(query)
            // FIXME: Assert firstTable is fully qualified to a known database
        } catch(ex: ParsingException){
            throw RestException(ex, ex.javaClass.simpleName, 400, ex.message, null, null)
        }

        val queryId = QUERY_ID_INT.incrementAndGet()
        val queryTask = QueryTask(query)
        val queryTaskFuture = EXECUTOR.submit(queryTask)
        QUERY_DATABASE[queryId] = queryTaskFuture
        val createdUri = uri.requestUriBuilder.path(queryId.toString()).build()
        return Response.seeOther(createdUri).build()
    }

    @GET
    @Path("{id}")
    @Produces(APPLICATION_JSON)
    fun getQuery(@PathParam("id") queryId : Int) : Response {
        val queryTaskFuture = QUERY_DATABASE[queryId]
        if (queryTaskFuture != null){
            return Response.ok().entity(queryTaskFuture.get().entity).build()
        }
        return Response.status(Response.Status.NOT_FOUND).build()
    }

    class QueryTask(val query: String) : Callable<QueryTask> {
        var entity : AsyncResponse? = null

        override fun call() : QueryTask {
            val statement: Statement
            // Try to parse query first
            try {
                statement = SqlParser().createStatement(query)
            } catch(ex: ParsingException){
                throw RestException(ex, ex.javaClass.simpleName, 400, ex.message, null, null)
            }

            val tableColumnExtractor = Analyzer.TableAndColumnExtractor()
            statement.accept(tableColumnExtractor, null)
            val relations = tableColumnExtractor.relations
            val columns = tableColumnExtractor.columns
            val tables = arrayListOf<QualifiedName>()
            for (relation in relations) {
                if(relation is Table){
                    tables.add(relation.name)
                }
            }

            val mdTask = LookupMetadataTask(tableColumnExtractor)
            val mdTaskFuture = EXECUTOR.submit(mdTask)

            val connectionUrl = System.getProperty("DATABASE_URL")
            val user = System.getProperty("DATABASE_USER")
            val password = System.getProperty("DATABASE_PASSWORD")

            val conn = DriverManager.getConnection(connectionUrl, user, password)
            val rowIterator = RowIterator(conn, query)
            val columnMetadataList : ArrayList<ColumnMetadata> = arrayListOf()

            mdTaskFuture.get()
            for((name, md) in rowIterator.jdbcColumnMetadata){
                val lookupColumnMetadata = mdTask.columnMetadata.get(name)
                val columnMetadata =
                        ColumnMetadata(name, datatype=jdbcToLsstType(md.jdbcType), ucd=lookupColumnMetadata?.ucd)
                columnMetadataList.add(columnMetadata)
            }

            entity = AsyncResponse(
                metadata = ResponseMetadata(columnMetadataList),
                results = rowIterator.asSequence().toList()
            )
            return this
        }
    }

    fun getFirstTable(query: String): QualifiedName? {
        // Try to parse query first
        val statement = SqlParser().createStatement(query)
        val tableColumnExtractor = Analyzer.TableAndColumnExtractor()
        statement.accept(tableColumnExtractor, null)
        val relations = tableColumnExtractor.relations
        for (relation in relations) {
            if(relation is Table){
                return relation.name
            }
        }
        return null
    }

}