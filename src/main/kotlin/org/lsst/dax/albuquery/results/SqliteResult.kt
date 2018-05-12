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

package org.lsst.dax.albuquery.results

import liquibase.Contexts
import liquibase.Liquibase
import liquibase.change.ColumnConfig
import liquibase.change.core.CreateTableChange
import liquibase.changelog.ChangeLogParameters
import liquibase.changelog.DatabaseChangeLog
import liquibase.database.DatabaseFactory
import liquibase.exception.ChangeLogParseException
import liquibase.parser.core.yaml.YamlChangeLogParser
import liquibase.resource.ClassLoaderResourceAccessor
import liquibase.resource.CompositeResourceAccessor
import liquibase.resource.FileSystemResourceAccessor
import liquibase.resource.ResourceAccessor

import org.lsst.dax.albuquery.JdbcColumnMetadata

class SqliteResult {
    companion object {
        fun buildChangeLog(jdbcColumnMetadata: Collection<JdbcColumnMetadata>): DatabaseChangeLog {
            val createTableChange = CreateTableChange()
            createTableChange.tableName = "result"
            for (column in jdbcColumnMetadata) {
                val columnConfig = ColumnConfig()
                columnConfig.name = column.name
                columnConfig.type = column.jdbcType.toString()
                createTableChange.addColumn(columnConfig)
            }
            val changeLogParameters = ChangeLogParameters()
            val accessor = CompositeResourceAccessor(ClassLoaderResourceAccessor(), FileSystemResourceAccessor())
            val pkgPath = ::SqliteResult.javaClass.`package`.name.replace(".", "/")
            val schemaFileName = "schema.yml"
            var schemaResource = pkgPath + "/" + schemaFileName
            val parser = YamlChangeLogParser()

            var changeLog: DatabaseChangeLog
            try {
                changeLog = parser.parse(schemaResource, changeLogParameters, accessor)
            } catch (ex: ChangeLogParseException) {
                // This is for the unit tests. schema.yml is addressed (properly) as a resource
                schemaResource = ::SqliteResult.javaClass.getResource(schemaFileName).file
                changeLog = parser.parse(schemaResource, changeLogParameters, accessor)
            }
            changeLog.changeSets[0].addChange(createTableChange)
            return changeLog
        }

        fun initializeDatabase(changeLog: DatabaseChangeLog, dbUri: String) {
            val accessor: ResourceAccessor? = FileSystemResourceAccessor()
            val liquibaseDb = DatabaseFactory.getInstance().openDatabase(dbUri, null, null, null, accessor)
            val lb = Liquibase(changeLog, accessor, liquibaseDb)
            lb.update(Contexts())
        }
    }
}
