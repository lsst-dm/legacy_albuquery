package org.lsst.dax.albuquery.resources

import com.codahale.metrics.annotation.Timed
import com.facebook.presto.sql.SqlFormatter.formatSql
import com.facebook.presto.sql.parser.ParsingException
import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.tree.QualifiedName
import com.facebook.presto.sql.tree.Query
import com.facebook.presto.sql.tree.QuerySpecification
import com.facebook.presto.sql.tree.Table
import org.lsst.dax.albuquery.*
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.rewrite.TableNameRewriter
import java.util.*
import java.util.concurrent.Callable
import java.util.logging.Logger
import javax.ws.rs.core.MediaType.APPLICATION_JSON
import javax.ws.rs.core.Context
import javax.ws.rs.core.Response
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Future
import javax.ws.rs.*
import javax.ws.rs.core.UriInfo

data class AsyncResponse(val metadata: ResponseMetadata,
                         val results: List<List<Any>>)

data class ResponseMetadata(val columns: List<ColumnMetadata>)

val DBURI = Regex("//.*")

@Path("async")
class AsyncResource(val metaservDAO: MetaservDAO) {

    private val LOGGER = Logger.getLogger("AsyncResource")
    @Context lateinit var uri : UriInfo
    val QUERY_DATABASE = ConcurrentHashMap<String, Future<QueryTask>>()

    init {
        // Initialize webserver stuff
    }

    @POST
    fun createQuery(query: String): Response {
        val querySpecification : QuerySpecification
        try {
            val statement = SqlParser().createStatement(query)
            if (statement !is Query) {
                val err = ErrorResponse("Only Select Queries allowed", "NotSelectStatementException", null, null)
                return Response.status(Response.Status.BAD_REQUEST).entity(err).build()
            }
            querySpecification = statement.queryBody as QuerySpecification
        } catch(ex: ParsingException){
            val err = ErrorResponse(ex.errorMessage, ex.javaClass.simpleName, null, cause = ex.message)
            return Response.status(Response.Status.BAD_REQUEST).entity(err).build()
        }

        // FIXME: Assert firstTable is fully qualified to a known database
        val queryId = UUID.randomUUID().toString()
        val queryTask = QueryTask(metaservDAO, querySpecification)
        val queryTaskFuture = EXECUTOR.submit(queryTask)
        QUERY_DATABASE[queryId] = queryTaskFuture
        val createdUri = uri.requestUriBuilder.path(queryId).build()
        return Response.seeOther(createdUri).build()
    }

    @Timed
    @GET
    @Path("{id}")
    @Produces(APPLICATION_JSON)
    fun getQuery(@PathParam("id") queryId : String) : Response {
        val queryTaskFuture = QUERY_DATABASE[queryId]
        if (queryTaskFuture != null){
            return Response.ok().entity(queryTaskFuture.get().entity).build()
        }
        return Response.status(Response.Status.NOT_FOUND).build()
    }

    class QueryTask(val metaservDAO: MetaservDAO, val querySpecification: QuerySpecification) : Callable<QueryTask> {
        var entity : AsyncResponse? = null

        override fun call() : QueryTask {
            val tableColumnExtractor = Analyzer.TableAndColumnExtractor()
            querySpecification.accept(tableColumnExtractor, null)
            val extractedRelations = tableColumnExtractor.relations
            val extractedColumns = tableColumnExtractor.columns


            val tables = arrayListOf<QualifiedName>()
            for (relation in extractedRelations) {
                if(relation is Table){
                    tables.add(relation.name)
                }
            }

            val firstTable = Analyzer.getFirstTable(extractedRelations)
            if (firstTable.parts.size < 3){
                throw ParsingException("No database instance identified: ${firstTable}")
            }
            val instanceIdentifier = firstTable.parts.get(0)
            val dbUri = Analyzer.getDatabaseURI(metaservDAO, instanceIdentifier)

            // Submit Metadata task
            val mdTask = LookupMetadataTask(metaservDAO, extractedRelations, extractedColumns)
            val mdTaskFuture = EXECUTOR.submit(mdTask)

            // Rewrite query to extract database instance information
            val rewriter = TableNameRewriter()
            val rewrittenQuery = rewriter.process(querySpecification)

            // Submit for data processing
            var query = formatSql(rewrittenQuery, Optional.empty())
            // FIXME: MySQL specific hack because we can't coax Qserv to ANSI compliance
            query = query.replace("\"", "`")

            mdTaskFuture.get()
            val conn = getConnection(dbUri)

            val rowIterator = RowIterator(conn, query)
            val columnMetadataList : ArrayList<ColumnMetadata> = arrayListOf()
            //mdTaskFuture.get()

            for((name, md) in rowIterator.jdbcColumnMetadata){
                val schemaName = md.schemaName ?: md.catalogName
                val qualifiedName = QualifiedName.of(schemaName, md.tableName)
                val metaservColumns = mdTask.columnMetadata.get(qualifiedName)
                        ?.associateBy({it.name}, {it})
                val metaservColumn = metaservColumns?.get(name)
                val columnMetadata =
                        ColumnMetadata(name,
                                datatype = metaservColumn?.datatype ?: jdbcToLsstType(md.jdbcType),
                                ucd = metaservColumn?.ucd,
                                unit = metaservColumn?.unit,
                                jdbcType = md.jdbcType)
                columnMetadataList.add(columnMetadata)
            }

            entity = AsyncResponse(
                    metadata = ResponseMetadata(columnMetadataList),
                    results = rowIterator.asSequence().toList()
            )
            return this
        }
    }

}