package org.lsst.dax.albuquery.results

import liquibase.Contexts
import liquibase.Liquibase
import liquibase.change.ColumnConfig
import liquibase.change.core.CreateTableChange
import liquibase.changelog.ChangeLogParameters
import liquibase.changelog.DatabaseChangeLog
import liquibase.database.DatabaseFactory
import liquibase.parser.core.yaml.YamlChangeLogParser
import liquibase.resource.FileSystemResourceAccessor
import liquibase.resource.ResourceAccessor

import org.lsst.dax.albuquery.JdbcColumnMetadata

class SqliteResult {
    companion object {
        fun buildChangeLog(jdbcColumnMetadata: List<JdbcColumnMetadata>): DatabaseChangeLog {
            val createTableChange = CreateTableChange()
            createTableChange.tableName = "result"
            for (column in jdbcColumnMetadata) {
                val columnConfig = ColumnConfig()
                columnConfig.name = column.name
                columnConfig.type = column.jdbcType.toString()
                createTableChange.addColumn(columnConfig)
            }
            val changeLogParameters = ChangeLogParameters()
            val schemaResource = ::SqliteResult.javaClass.getResource("schema.yml")
            val resourceAccessor: ResourceAccessor = FileSystemResourceAccessor()
            val parser = YamlChangeLogParser()
            val changeLog = parser.parse(schemaResource.file, changeLogParameters, resourceAccessor)
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
