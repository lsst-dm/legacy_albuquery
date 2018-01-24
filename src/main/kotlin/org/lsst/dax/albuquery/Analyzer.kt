package org.lsst.dax.albuquery
import com.facebook.presto.sql.tree.*

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
}