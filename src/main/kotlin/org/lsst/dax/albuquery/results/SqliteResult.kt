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

            var changeLog : DatabaseChangeLog
            try {
                changeLog = parser.parse(schemaResource, changeLogParameters, accessor)
            } catch (ex: ChangeLogParseException) {
                // This is for the unit tests. schema.yml is addressed (properly) as a resource
                schemaResource =  ::SqliteResult.javaClass.getResource(schemaFileName).file
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
