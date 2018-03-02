package org.lsst.dax.albuquery

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.dropwizard.Application
import io.dropwizard.jdbi3.JdbiFactory
import io.dropwizard.setup.Bootstrap
import io.dropwizard.setup.Environment
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.resources.AsyncResource
import java.util.concurrent.Executors
import javax.ws.rs.ext.ContextResolver

val EXECUTOR = Executors.newCachedThreadPool()
var CONFIG: AlbuqueryConfiguration? = null
lateinit var SERVICE_ACCOUNT_CONNECTIONS: ServiceAccountConnections

class AlbuqueryApplication() : Application<AlbuqueryConfiguration>() {

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            AlbuqueryApplication().run(*args)
        }
    }

    override fun initialize(bootstrap: Bootstrap<AlbuqueryConfiguration>?) {
        // This enables deserialization to kotlin objects in configuration
        bootstrap?.objectMapper?.registerModule(KotlinModule())
    }

    override fun run(config: AlbuqueryConfiguration, env: Environment) {
        CONFIG = config
        SERVICE_ACCOUNT_CONNECTIONS = ServiceAccountConnections(config.DAX_PASSWORD_STORE)

        //val healthCheck = TemplateHealthCheck(config.template)
        //env.healthChecks().register("template", healthCheck)
        val factory = JdbiFactory()
        val jdbi = factory.build(env, config.DAX_METASERV_DB, "mysql")
        jdbi.installPlugin(KotlinPlugin())
        val metaservDAO = jdbi.onDemand(MetaservDAO::class.java)
        val resource = AsyncResource(metaservDAO)
        env.jersey().register(resource)
        env.jersey().register(ContextResolver<ObjectMapper> { ObjectMapper().registerModule(KotlinModule()) })
    }
}
