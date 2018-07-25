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
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.vo.TableMapper
import org.slf4j.LoggerFactory
import javax.ws.rs.FormParam
import javax.ws.rs.POST
import javax.ws.rs.Path
import javax.ws.rs.QueryParam
import javax.ws.rs.core.*

@Path("sync")
class Sync(val metaservDAO: MetaservDAO) {

    @Context
    lateinit var uri: UriInfo
    @Context
    lateinit var headers: HttpHeaders

    @Timed
    @POST
    fun createQuery(@QueryParam("query") @FormParam("query") queryParam: String?, postBody: String): Response {
        val query = queryParam ?: postBody
        LOGGER.info("Recieved query [$query]")
        val ct = headers.getRequestHeader(HttpHeaders.ACCEPT).get(0)
        val om: ObjectMapper
        if (ct == MediaType.APPLICATION_XML)
            om =  TableMapper()
        else
            om = ObjectMapper().registerModule(KotlinModule())
        return Async.createAsyncQuery(metaservDAO, uri, query, om, resultRedirect = true)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(Async::class.java)
    }
}
