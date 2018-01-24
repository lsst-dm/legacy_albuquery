package org.lsst.dax.albuquery

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.glassfish.jersey.logging.LoggingFeatureAutoDiscoverable
import org.glassfish.jersey.server.ResourceConfig
import java.net.URI
import java.util.logging.Logger
import javax.ws.rs.ext.ContextResolver
import org.glassfish.jersey.jetty.JettyHttpContainerFactory

object JettyServer {

    private val LOGGER = Logger.getLogger("JettyServer")

    @JvmStatic
    fun main(args: Array<String>) {
        val resourceConfig = ResourceConfig.forApplication(JerseyApplication())
                .register(LoggingFeatureAutoDiscoverable())
                .register(ContextResolver<ObjectMapper> { ObjectMapper().registerModule(KotlinModule()) })

        val server = JettyHttpContainerFactory.createServer(URI.create("http://localhost:8080/"), resourceConfig, null)

        Runtime.getRuntime().addShutdownHook(Thread(Runnable { server.join() }))
    }
}
