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
