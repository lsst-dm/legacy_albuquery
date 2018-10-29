/* This file is part of albuquery.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

package org.lsst.dax.albuquery.rewrite

import com.facebook.presto.sql.tree.BooleanLiteral
import com.facebook.presto.sql.tree.Node
import com.facebook.presto.sql.tree.QualifiedName
import com.facebook.presto.sql.tree.ShowColumns
import com.facebook.presto.sql.tree.Table

class TableNameRewriter() : AstRebuilder<Void?>() {

    public var hasBooleanLiterals: Boolean = false

    override fun visitTable(node: Table, context: Void?): Node? {
        val name = node.name
        if (name.originalParts.size == 3) {
            val strippedName = name.originalParts.subList(1, name.originalParts.size)
            return Table(QualifiedName.of(strippedName))
        }
        return node
    }

    override fun visitShowColumns(node: ShowColumns?, context: Void?): Node {
        val tableName = node?.getTable().toString()
        val strippedName = tableName.toString().substringAfter(".")
        return ShowColumns(QualifiedName.of(strippedName))
    }

    override fun visitBooleanLiteral(node: BooleanLiteral?, context: Void?): Node {
        if (node != null) {
            this.hasBooleanLiterals = true
            val value: Boolean = node.value
            return BooleanLiteral(value.toString())
        } else {
            return BooleanLiteral(null)
        }
    }
}
