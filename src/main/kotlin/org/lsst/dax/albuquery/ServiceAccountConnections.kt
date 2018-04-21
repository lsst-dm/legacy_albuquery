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

import org.slf4j.LoggerFactory
import java.net.URI
import java.sql.Connection
import java.sql.DriverManager

class ServiceAccountConnections(serviceAccountCredentials: List<Credential>) {
    val hostPortMap: HashMap<String, Credential> = hashMapOf()

    init {
        for (credential in serviceAccountCredentials) {
            hostPortMap[credential.server + credential.port] = credential
        }
    }

    fun getConnection(connectionUri: URI): Connection {
        LOGGER.debug("Getting connection for URI: $connectionUri")
        // FIXME: Can't do much about this for now unless we store passwords in the db
        val uri = "jdbc:" + connectionUri
        val credential = hostPortMap[connectionUri.host + connectionUri.port]
        return DriverManager.getConnection(uri, credential?.username, credential?.password)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ServiceAccountConnections::class.java)
    }
}
