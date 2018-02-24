package org.lsst.dax.albuquery.rewrite

import com.facebook.presto.sql.parser.ParsingOptions
import com.facebook.presto.sql.parser.SqlParser

import org.junit.Test

class VisitorTest{

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