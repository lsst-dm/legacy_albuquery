package org.lsst.dax.albuquery

import com.facebook.presto.sql.tree.QualifiedName
import org.lsst.dax.albuquery.model.metaserv.Column
import org.lsst.dax.albuquery.model.metaserv.Table
import java.util.LinkedHashMap

class QueryMetadataHelper(val analyzer: Analyzer.TableAndColumnExtractor) {

    fun associateMetadata(
        jdbcColumnMetadata: LinkedHashMap<String, JdbcColumnMetadata>,
        metaservColumns: Map<Table, List<Column>>
    ): ArrayList<ColumnMetadata> {

        val parsedTableMapping = buildTableNameAndAliasMapping()
        val parsedTableToMetaservColumns = linkedMapOf<QualifiedName, Map<String, Column>>()

        // Build up metaserv column mappings.
        for ((table, columns) in metaservColumns) {
            // Only process tables we've found from the parse
            val parsedTable = parsedTableMapping[QualifiedName.of(table.name)]
            val columnMap = columns.associateBy({ it.name }, { it })
            if (parsedTable != null) {
                parsedTableToMetaservColumns[parsedTable.qualifiedName] = columnMap
            }
        }

        val parsedTableToParsedColumnMap = hashMapOf<ParsedTable, HashMap<String, ParsedColumn>>()
        for (parsedColumn in analyzer.columns) {
            if (!parsedColumn.qualifiedName.prefix.isPresent) {
                continue
            }
            val tableName = parsedColumn.qualifiedName.prefix.get()
            val table = parsedTableMapping[tableName]
            if (table != null) {
                val columnMap = parsedTableToParsedColumnMap.getOrPut(table) { hashMapOf() }
                columnMap[parsedColumn.identifier] = parsedColumn
                if (parsedColumn.alias != null) {
                    columnMap[parsedColumn.alias] = parsedColumn
                }
            }
        }

        val defaultMetaservColumnNameMapping = hashMapOf<String, Column>()
        // parsedTableToMetaservColumns is a LinkedHashMap, so we are iterating left-to-right over the tables
        for (metaservColumnsMap in parsedTableToMetaservColumns.values) {
            for (column in metaservColumnsMap.values) {
                if (column.name !in defaultMetaservColumnNameMapping) {
                    defaultMetaservColumnNameMapping[column.name] = column
                }
            }
        }

        val columnPositionMapping = analyzer.columns.associateBy({ it.position }, { it })
        val columnMetadataList: ArrayList<ColumnMetadata> = arrayListOf()

        for ((name, jdbcColumn) in jdbcColumnMetadata) {
            val parsedColumn = columnPositionMapping[jdbcColumn.ordinal]
            var metaservColumn: Column? = null

            if (parsedColumn != null) {
                var parsedTable: ParsedTable? = null

                for ((table, columnMap) in parsedTableToParsedColumnMap) {
                    if (parsedColumn.identifier in columnMap) {
                        parsedTable = table
                    }
                }

                // If we found a table, try that
                if (parsedTable != null) {
                    val metaservColumnsMap = parsedTableToMetaservColumns[parsedTable.qualifiedName]
                    metaservColumn = metaservColumnsMap?.get(parsedColumn.identifier)
                }

                // If it's still null, try the default map with the original identifier
                if (metaservColumn == null) {
                    metaservColumn = defaultMetaservColumnNameMapping[parsedColumn.identifier]
                }
            }

            if (metaservColumn == null) {
                if (name in defaultMetaservColumnNameMapping) {
                    metaservColumn = defaultMetaservColumnNameMapping[name]
                }
            }

            val columnMetadata =
                    ColumnMetadata(name,
                            datatype = metaservColumn?.datatype ?: jdbcToLsstType(jdbcColumn.jdbcType),
                            ucd = metaservColumn?.ucd,
                            unit = metaservColumn?.unit,
                            tableName = metaservColumn?.tableName ?: jdbcColumn.tableName,
                            jdbcType = jdbcColumn.jdbcType)
            columnMetadataList.add(columnMetadata)
        }
        return columnMetadataList
    }

    fun buildTableNameAndAliasMapping(): Map<QualifiedName, ParsedTable> {
        val tableNameMapping = hashMapOf<QualifiedName, ParsedTable>()
        for (table in analyzer.tables) {
            if (table.alias != null) {
                tableNameMapping[QualifiedName.of(table.alias)] = table
            }
            val parts = arrayListOf<String>()
            for (part in table.qualifiedName.originalParts.reversed()) {
                parts.add(part)
                tableNameMapping[QualifiedName.of(parts.reversed())] = table
            }
        }
        return tableNameMapping
    }
}
