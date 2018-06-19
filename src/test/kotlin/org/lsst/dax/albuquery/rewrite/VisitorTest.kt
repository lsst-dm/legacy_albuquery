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

import com.facebook.presto.sql.SqlFormatter
import com.facebook.presto.sql.parser.ParsingOptions
import com.facebook.presto.sql.parser.SqlParser
import com.facebook.presto.sql.tree.Node

import org.junit.Test
import java.util.LinkedList
import java.util.Optional

class VisitorTest {

    @Test
    fun testTableNameRewriter() {
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

    @Test
    fun testSciSqlRewriter() {
        val parsingOptions = ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE)
        //var sql = "SELECT * from foo WHERE CONTAINS(POINT(1,2), CIRCLE(1,2,3))"
        val rewriter = AdqlSciSqlRewriter(true)
        var sql = "SELECT * from Object o WHERE 1=CONTAINS(POINT(o.ra, o.decl), CIRCLE(1,2,3))"
        var stmt = SqlParser().createStatement(sql, parsingOptions)

        val stack = LinkedList<Node>()
        println(sql)
        println(SqlFormatter.formatSql(rewriter.process(stmt, stack), Optional.empty()))

        sql = "SELECT * from Object o WHERE CONTAINS(POINT(o.ra, o.decl), BOX(20,25,10,8))"
        stmt = SqlParser().createStatement(sql, parsingOptions)
        println(sql)
        println(SqlFormatter.formatSql(rewriter.process(stmt, stack), Optional.empty()))

        sql = "SELECT * from Object o WHERE 1=CONTAINS(POINT(o.ra, o.decl), BOX(20,25,10,8))"
        stmt = SqlParser().createStatement(sql, parsingOptions)
        println(sql)
        println(SqlFormatter.formatSql(rewriter.process(stmt, stack), Optional.empty()))

        sql = "SELECT * from Object o WHERE 1=CONTAINS(POINT(o.ra, o.decl), POLYGON(20,25,10,8,0,0))"
        stmt = SqlParser().createStatement(sql, parsingOptions)
        println(sql)
        println(SqlFormatter.formatSql(rewriter.process(stmt, stack), Optional.empty()))

        sql = "SELECT * from W13_sdss_v2.sdss_stripe82_01.RunDeepSource " +
            "WHERE 1=contains(point(ra, dec), box(9.5,-1.23,9.6,-1.22))"
        stmt = SqlParser().createStatement(sql, parsingOptions)
        println(sql)
        println(SqlFormatter.formatSql(rewriter.process(stmt, stack), Optional.empty()))

        sql = "SELECT * from W13_sdss_v2.sdss_stripe82_01.RunDeepSource " +
            "WHERE contains(point(ra, dec), box(9.5,-1.23,9.6,-1.22))" +
            "AND DISTANCE(ra, dec, o.ra, o.dec) < 1"
        stmt = SqlParser().createStatement(sql, parsingOptions)
        println(sql)
        println(SqlFormatter.formatSql(rewriter.process(stmt, stack), Optional.empty()))

        sql =
            """SELECT * from W13_sdss_v2.sdss_stripe82_01.RunDeepSource
                WHERE
                contains(point(ra, dec), box(9.5, -1.23, 9.6, -1.22))
                AND contains(point(ra, dec), polygon(1, 2, 9.5, -1.23, 9.6, -1.22))
                AND contains(point(ra, dec), circle(1, 2, 3))
                AND DISTANCE(ra, dec, o.ra, o.dec) < 1
                AND DISTANCE(point(ra, dec), point(o.ra, o.dec)) < 2"""
        stmt = SqlParser().createStatement(sql, parsingOptions)
        println(sql)
        println(SqlFormatter.formatSql(rewriter.process(stmt, stack), Optional.empty()))
    }
}
