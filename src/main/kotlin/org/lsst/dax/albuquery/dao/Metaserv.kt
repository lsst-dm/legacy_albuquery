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

package org.lsst.dax.albuquery.dao

import org.jdbi.v3.sqlobject.statement.SqlQuery
import org.lsst.dax.albuquery.model.metaserv.Column
import org.lsst.dax.albuquery.model.metaserv.Database
import org.lsst.dax.albuquery.model.metaserv.Schema
import org.lsst.dax.albuquery.model.metaserv.Table

// See: http://jdbi.org/#_kotlin
interface MetaservDAO {

    @SqlQuery("select id, name, description, conn_host, conn_port from MSDatabase where name = ?")
    fun findDatabaseByName(name: String): Database?

    @SqlQuery("select * from MSDatabaseSchema where db_id = ? and is_default_schema = 1")
    fun findDefaultSchemaByDatabaseId(dbId: Int): Schema?

    @SqlQuery("select * from MSDatabaseTable where schema_id = ?")
    fun findTablesBySchemaId(schemaId: Int): List<Table>?

    @SqlQuery("select * from MSDatabaseColumn where table_id = ?")
    fun findColumnsByTableId(talbleId: Int): List<Column>
}
