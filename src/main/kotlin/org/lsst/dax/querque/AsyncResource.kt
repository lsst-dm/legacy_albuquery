package org.lsst.dax.querque

import javax.ws.rs.DELETE
import javax.ws.rs.GET
import javax.ws.rs.POST
import javax.ws.rs.PUT
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType.APPLICATION_JSON
//import javax.ws.rs.core.Application
//import javax.ws.rs.core.Context

@Path("async")
@Produces(APPLICATION_JSON)
class AsyncResource {

    //@Context
    //lateinit var application: Application

    init {
        // Initialize webserver stuff
    }

    @POST
    fun createQuery(query: String): String? {
        val mdTask = EXECUTOR.submit(RetrieveMetadataTask(query))
        val md = mdTask.get()
        return md.values.toString()
    }

}