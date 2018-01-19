package org.lsst.dax.querque

import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.tree.DereferenceExpression
import com.facebook.presto.sql.tree.Identifier
import com.facebook.presto.sql.tree.QualifiedName
import com.facebook.presto.sql.tree.Relation
import java.util.concurrent.Callable

data class ColumnMetadata(val qualifiedName: QualifiedName,
                          val alias: String?,
                          var datatype: String?,
                          var ucd: String?)


class RetrieveMetadataTask(val query: String) : Callable<Map<QualifiedName, ColumnMetadata>> {
    lateinit var tables: List<Relation>

    override fun call(): Map<QualifiedName, ColumnMetadata> {
        val statement = SqlParser().createStatement(query)
        val tableColumnExtractor = Analyzer.TableAndColumnExtractor()
        statement.accept(tableColumnExtractor, null)
        tables = tableColumnExtractor.tables
        return tableColumnExtractor.columns
    }

}
