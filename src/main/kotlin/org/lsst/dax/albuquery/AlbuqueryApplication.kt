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

package org.lsst.dax.albuquery

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.dropwizard.Application
import io.dropwizard.jdbi3.JdbiFactory
import io.dropwizard.setup.Bootstrap
import io.dropwizard.setup.Environment
import org.jdbi.v3.core.kotlin.KotlinPlugin
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.resources.Async
import org.lsst.dax.albuquery.resources.Sync
import java.nio.file.Files
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
        if (CONFIG?.DAX_BASE_PATH == null) {
            val base_path = Files.createTempDirectory("albuquery")
            CONFIG?.DAX_BASE_PATH = base_path.toString()
        }
        println()
        println("TEMP DIR AT " + CONFIG?.DAX_BASE_PATH)
        // val healthCheck = TemplateHealthCheck(config.template)
        // env.healthChecks().register("template", healthCheck)
        val factory = JdbiFactory()
        val jdbi = factory.build(env, config.DAX_METASERV_DB, "mysql")
        jdbi.installPlugin(KotlinPlugin())
        val metaservDAO = jdbi.onDemand(MetaservDAO::class.java)
        env.jersey().register(Async(metaservDAO))
        env.jersey().register(Sync(metaservDAO))
        env.jersey().register(ContextResolver<ObjectMapper> { ObjectMapper().registerModule(KotlinModule()) })
    }
}
