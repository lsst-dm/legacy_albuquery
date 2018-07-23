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

package org.lsst.dax.albuquery.vo

import java.io.Writer

import com.fasterxml.jackson.databind.ObjectMapper

import org.lsst.dax.albuquery.ErrorResponse
import org.lsst.dax.albuquery.ColumnMetadata
import org.lsst.dax.albuquery.resources.Async.AsyncResponse

fun writeField(w: Writer, field: ColumnMetadata) {
    var str: String = "<FIELD"
    str.plus(" name=" + field.name)
    str.plus(" ucd=" + field.ucd)
    str.plus(" datatype=" + field.datatype)
    str.plus(" unit=" + field.unit)
    str.plus(" description=" + field.description)
    str.plus(" />")
    w.write(str)
}

fun writeRow(w: Writer, row: List<Any?>) {
    var str: String = "<TR>"
    for (col in row) {
        str.plus("<TD>")
        str.plus(col)
        str.plus("</TD>")
    }
    str.plus("</TR>")
    w.write(str)
}

class TableMapper() : ObjectMapper() {

    override fun writeValue(w: Writer, entity: Any) {
        var str: String
        if (entity is ErrorResponse) {
            val err = entity as ErrorResponse
            str = "<Error>"
            str.plus("<Message>")
            str.plus(err.message)
            str.plus("<type>")
            str.plus(err.type)
            str.plus("<code>")
            str.plus(err.code)
            str.plus("<cause>")
            str.plus(err.cause)
            w.write(str)
            return
        }
        if (entity !is AsyncResponse) return
        val ar = entity as AsyncResponse
        var fields: List<ColumnMetadata> = ar.metadata.columns
        var rowIterator: Iterator<List<Any?>> = ar.results
        str = "<?xml version=\"1.0\"?>"
        str.plus("<VOTABLE version=\"1.3\" xmlns=\"http://www.ivoa.net/xml/VOTable/v1.3\">")
        str.plus("<RESOURCE name=\"Result for query: \"")
        str.plus(ar.queryId)
        str.plus("type=\"meta\">")
        str.plus("<TABLE name=\"")
        str.plus("") // FIXME:
        str.plus(">")
        str.plus("<DESCRIPTION>")
        str.plus("Query Results")
        str.plus("</DESCRIPTION>")
        w.write(str)
        for (field in fields ) {
            writeField(w, field)
        }
        str = "<DATA><TABLEDATA>"
        str.plus("<TR>")
        w.write(str)
        // process the data rows
        for (row in rowIterator) {
            writeRow(w, row)
        }
        str = "</TR>"
        str.plus("</TABLEDATA></DATA>")
        str.plus("</TABLE>")
        str.plus("</RESOURCE>")
        str.plus("</VOTABLE>")
        w.write(str)
    }
}
