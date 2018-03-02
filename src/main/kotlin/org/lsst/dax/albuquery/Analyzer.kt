package org.lsst.dax.albuquery

import com.facebook.presto.sql.parser.ParsingException
import com.facebook.presto.sql.tree.AliasedRelation
import com.facebook.presto.sql.tree.QualifiedName
import com.facebook.presto.sql.tree.DefaultTraversalVisitor
import com.facebook.presto.sql.tree.Identifier
import com.facebook.presto.sql.tree.QuerySpecification
import com.facebook.presto.sql.tree.AllColumns
import com.facebook.presto.sql.tree.DereferenceExpression
import com.facebook.presto.sql.tree.Join
import com.facebook.presto.sql.tree.Relation
import com.facebook.presto.sql.tree.SingleColumn
import com.facebook.presto.sql.tree.SubqueryExpression
import com.facebook.presto.sql.tree.Table
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.resources.DBURI
import java.net.URI
import javax.ws.rs.core.UriBuilder

class Analyzer {

    class TableAndColumnExtractor : DefaultTraversalVisitor<Void, Void>() {
        val columns = arrayListOf<ParsedColumn>()
        val allColumnTables = arrayListOf<QualifiedName>()
        val tables = arrayListOf<ParsedTable>()
        var allColumns = false

        override fun visitSubqueryExpression(node: SubqueryExpression?, context: Void?): Void? {
            return null
        }

        override fun visitQuerySpecification(node: QuerySpecification, context: Void?): Void? {
            val relations = arrayListOf<Relation>()
            for ((index, item) in node.select.selectItems.withIndex()) {
                val position = index + 1

                if (item is SingleColumn) {
                    val column = item
                    val expression = column.expression
                    var qualifiedName: QualifiedName? = null
                    when (expression) {
                        is Identifier -> {
                            qualifiedName = QualifiedName.of(expression.value)
                        }
                        is DereferenceExpression -> {
                            qualifiedName = DereferenceExpression.getQualifiedName(expression)
                        }
                    }
                    if (qualifiedName != null) {
                        //val name = qualifiedName.toString()
                        val alias = column.alias.orElse(null)?.value
                        columns.add(ParsedColumn(nameOf(qualifiedName), qualifiedName, alias, position))
                    }
                }

                if (item is AllColumns) {
                    if (item.prefix.isPresent) {
                        allColumnTables.add(item.prefix.get())
                        val parts = arrayListOf<String>()
                        parts.addAll(item.prefix.get().originalParts)
                        parts.add("*")
                        val qualifiedName = QualifiedName.of(parts)
                        columns.add(ParsedColumn(nameOf(qualifiedName), qualifiedName, null, position))
                    } else {
                        columns.add(ParsedColumn("*", QualifiedName.of("*"), null, position))
                        allColumns = true
                    }
                }
            }

            if (node.from.isPresent) {
                val from = node.from.get()
                var relation: Join
                relations.clear()
                if (from is Join) {
                    relation = from
                    relations.add(relation.right)
                    while (relation.left is Join) {
                        relation = relation.left as Join
                        relations.add(relation.right)
                    }
                    relations.add(relation.left)
                } else {
                    relations.add(from)
                }
            }
            relations.reverse()

            for (index in relations.indices) {
                val position = index + 1
                var relation = relations[index]
                var alias: String? = null
                if (relation is AliasedRelation) {
                    alias = relation.alias.value
                    relation = relation.relation
                }
                if (relation is Table) {
                    tables.add(ParsedTable(nameOf(relation.name), relation.name, alias, position))
                }
            }
            return null
        }
    }

    companion object {
        fun getDatabaseURI(metaservDAO: MetaservDAO, instanceIdentifier: String): URI? {
            // FIXME: MySQL specific
            val mysqlScheme = "mysql"
            var dbUri: URI? = null
            if (instanceIdentifier.matches(DBURI)) {
                val givenUri = URI(instanceIdentifier)
                dbUri = URI(mysqlScheme, null, givenUri.host, givenUri.port,
                    givenUri.path, null, null)
            }

            val db = metaservDAO.findDatabaseByName(instanceIdentifier)
            if (db != null) {
                val defaultSchema = metaservDAO.findDefaultSchemaByDatabaseId(db.id)
                dbUri = UriBuilder.fromPath(defaultSchema?.name ?: "")
                    .host(db.host)
                    .port(db.port)
                    .scheme(mysqlScheme)
                    .build()
            }
            if (dbUri == null) {
                // FIXME: Not really a parsing exception
                throw ParsingException("Unable to determine database to connect to")
            }
            return dbUri
        }

        fun findInstanceIdentifyingTable(relations: List<ParsedTable>): QualifiedName {
            var firstTable: QualifiedName? = null
            for (table in relations) {
                if (table.qualifiedName.parts.size == 3) {
                    firstTable = table.qualifiedName
                    break
                }
            }
            if (firstTable == null) {
                // Not sure if this can happen
                throw ParsingException("Unable to determine a table")
            }
            return firstTable
        }

        private fun nameOf(name: QualifiedName): String {
            val suffix = name.originalParts.last()
            return suffix
        }
    }
}
