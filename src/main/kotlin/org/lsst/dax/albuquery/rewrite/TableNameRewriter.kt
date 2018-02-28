package org.lsst.dax.albuquery.rewrite

import com.facebook.presto.sql.tree.Node
import com.facebook.presto.sql.tree.QualifiedName
import com.facebook.presto.sql.tree.Table

class TableNameRewriter : AstRebuilder<Void?>() {

    override fun visitTable(node: Table, context: Void?): Node? {
        val name = node.name
        if (name.originalParts.size == 3) {
            val strippedName = name.originalParts.subList(1, name.originalParts.size)
            return Table(QualifiedName.of(strippedName))
        }
        return node
    }
}
