package org.lsst.dax.albuquery.parser

import com.facebook.presto.sql.parser.ParsingOptions
import com.facebook.presto.sql.parser.SqlParser
import org.junit.Test

class ParsingTest {
    @Test
    fun testParsingFunction() {
        val parser = SqlParser()
        val parserOptions = ParsingOptions(ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE)
        parser.createStatement(
            "SELECT * from W13_sdss_v2.sdss_stripe82_01.RunDeepSource " +
            "WHERE qserv_areaspec_box(9.5,-1.23,9.6,-1.22)", parserOptions)
    }
}
