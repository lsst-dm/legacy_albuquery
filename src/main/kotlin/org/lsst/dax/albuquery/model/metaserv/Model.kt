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

package org.lsst.dax.albuquery.model.metaserv

import org.jdbi.v3.core.mapper.reflect.ColumnName

data class Database(
    val id: Int,
    val name: String,
    val description: String?,
    @ColumnName("conn_host") val host: String,
    @ColumnName("conn_port") val port: Int
)

data class Schema(
    val id: Int,
    @ColumnName("db_id") val dbId: Int,
    val name: String,
    val description: String?,
    @ColumnName("is_default_schema") val isDefaultSchema: Boolean
)

data class Table(
    val id: Int,
    @ColumnName("schema_id") val schemaId: Int,
    var schemaName: String?,
    val name: String,
    val description: String?
)

data class Column(
    val id: Int,
    @ColumnName("table_id") val table_id: Int,
    var tableName: String?,
    val name: String,
    val description: String?,
    val ordinal: Int?,
    val ucd: String?,
    val unit: String?,
    val datatype: String?,
    val nullable: Boolean?,
    val arraysize: Int?
)
