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

import com.fasterxml.jackson.annotation.JsonProperty
import io.dropwizard.Configuration
import io.dropwizard.db.DataSourceFactory

data class Credential(
    val server: String,
    val port: Int,
    val username: String?,
    val password: String?
)

class AlbuqueryConfiguration(
    @JsonProperty("dax_metaserv_db")
    val DAX_METASERV_DB: DataSourceFactory,

    @JsonProperty("dax_password_store")
    val DAX_PASSWORD_STORE: List<Credential>,

    @JsonProperty("dax_base_path")
    var DAX_BASE_PATH: String?

) : Configuration()
