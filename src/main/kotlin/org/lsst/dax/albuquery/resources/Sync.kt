package org.lsst.dax.albuquery.resources

import com.codahale.metrics.annotation.Timed
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.slf4j.LoggerFactory
import javax.ws.rs.Consumes
import javax.ws.rs.FormParam
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.QueryParam
import javax.ws.rs.core.Context
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import javax.ws.rs.core.UriInfo

@Path("sync")
class Sync(val metaservDAO: MetaservDAO) {

    @Context
    lateinit var uri: UriInfo

    @Timed
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    fun createQuery(@QueryParam("query") @FormParam("query") query: String): Response {
        LOGGER.info("Recieved query [$query]")
        val om = ObjectMapper().registerModule(KotlinModule())
        return Async.createAsyncQuery(metaservDAO, uri, query, om, true)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(Async::class.java)
    }
}
