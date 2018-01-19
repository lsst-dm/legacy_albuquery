package org.lsst.dax.querque
import com.facebook.presto.sql.tree.*

class Analyzer {


    class TableAndColumnExtractor : DefaultTraversalVisitor<Void, Void>(){
        val columns = hashMapOf<QualifiedName, ColumnMetadata>()
        val tables = arrayListOf<Relation>()

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
                        val md = ColumnMetadata(qualifiedName, alias,null, null)
                        columns[qualifiedName] = md
                    }
                }
            }

            if (node.from.isPresent){
                val from = node.from.get()
                tables.clear()
                if (from is Join){
                    var relation = from as Join
                    tables.add(relation.right)
                    while(relation.left is Join){
                        relation = relation.left as Join
                        tables.add(relation.right)
                    }
                    tables.add(relation.left)
                } else {
                    tables.add(from)
                }
            }
            tables.reverse()
            return null
        }

    }
}