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

import com.facebook.presto.sql.parser.ParsingOptions
import com.facebook.presto.sql.parser.SqlParser

import org.junit.Test

class VisitorTest {

    @Test
    fun firstTest() {
        println("Testing...")
        var sql = "SELECT a, \"B\" from \"//lsst:4040\".\"Sch\".Tab"
        var stmt = SqlParser().createStatement(sql, ParsingOptions())
        val rewriter = TableNameRewriter()
        println(rewriter.process(stmt, null))
        sql = "SELECT * from \"//lsst:4040\".\"Sch\".Tab"
        stmt = SqlParser().createStatement(sql, ParsingOptions())
        println(rewriter.process(stmt, null))

        sql = "SELECT * from \"//lsst:4040\".\"Sch\".Tab WHERE 2 = 1"
        stmt = SqlParser().createStatement(sql, ParsingOptions())
        println(rewriter.process(stmt, null))
    }
}
