package org.lsst.dax.albuquery
import com.facebook.presto.sql.parser.ParsingException
import com.facebook.presto.sql.tree.*
import org.lsst.dax.albuquery.dao.MetaservDAO
import org.lsst.dax.albuquery.resources.DBURI
import java.net.URI

class Analyzer {


    class TableAndColumnExtractor : DefaultTraversalVisitor<Void, Void>(){
        val columns = hashMapOf<QualifiedName, ParsedColumn>()
        val allColumnTables = arrayListOf<QualifiedName>()
        val relations = arrayListOf<Relation>()
        val tables = arrayListOf<Table>()
        var allColumns = false

        override fun visitQuerySpecification(node: QuerySpecification, context: Void?): Void? {
            for (item in node.select.selectItems){
                if (item is SingleColumn){
                    val column = item
                    val expression = column.expression
                    var qualifiedName : QualifiedName? = null
                    when (expression) {
                        is Identifier -> {
                            qualifiedName = QualifiedName.of(expression.value)

                        }
                        is DereferenceExpression -> {
                            qualifiedName = DereferenceExpression.getQualifiedName(expression)
                        }
                    }
                    if (qualifiedName != null){
                        //val name = qualifiedName.toString()
                        val alias = column.alias.orElse(null)?.value
                        val md = ParsedColumn(qualifiedName, alias)
                        columns[qualifiedName] = md
                    }
                }
                if(item is AllColumns){
                    if (item.prefix.isPresent){
                        allColumnTables.add(item.prefix.get())
                    } else {
                        allColumns = true
                    }
                }
            }

            if (node.from.isPresent){
                val from = node.from.get()
                var relation : Join
                relations.clear()
                if (from is Join){
                    relation = from
                    relations.add(relation.right)
                    while(relation.left is Join){
                        relation = relation.left as Join
                        relations.add(relation.right)
                    }
                    relations.add(relation.left)
                } else {
                    relations.add(from)
                }
            }
            relations.reverse()
            return null
        }

    }

    companion object {
        fun getDatabaseURI(metaservDAO: MetaservDAO, instanceIdentifier: String): String? {
            val jdbcPrefix = "jdbc:"
            // FIXME: MySQL specific
            val mysqlScheme = "mysql"
            var dbUri : URI? = null
            if (instanceIdentifier.matches(DBURI)){
                val givenUri = URI(instanceIdentifier)
                dbUri = URI(mysqlScheme, null, givenUri.host, givenUri.port,
                        givenUri.path, null, null)
            }

            val db = metaservDAO.findDatabaseByName(instanceIdentifier)
            if(db != null) {
                // FIXME: Might want to specify path based on default schema
                dbUri = URI(mysqlScheme, null, db.host, db.port,
                        null, null, null)
            }
            if (dbUri == null){
                // FIXME: Not really a parsing exception
                throw ParsingException("Unable to determine database to connect to")
            }
            return jdbcPrefix + dbUri
        }

        fun getFirstTable(relations: List<Relation>) : QualifiedName {
            var firstTable : QualifiedName? = null
            for (relation in relations) {
                if(relation is Table){
                    firstTable = relation.name
                    break
                }
            }
            if (firstTable == null){
                // Not sure if this can happen
                throw ParsingException("Unable to determine a table")
            }
            return firstTable
        }
    }
}