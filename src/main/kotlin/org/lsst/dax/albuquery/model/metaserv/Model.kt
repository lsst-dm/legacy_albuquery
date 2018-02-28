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
    val name: String,
    val description: String?
)

data class Column(
    val id: Int,
    @ColumnName("table_id") val table_id: Int,
    val name: String,
    val description: String?,
    val ordinal: Int?,
    val ucd: String?,
    val unit: String?,
    val datatype: String?,
    val nullable: Boolean?,
    val arraysize: Int?
)
