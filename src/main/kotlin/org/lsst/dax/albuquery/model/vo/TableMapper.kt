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
    var str = StringBuilder("<FIELD ")
    str.append("name=\"" + field.name)
    str.append("\" ucd=\"" + field.ucd)
    str.append("\" datatype=\"" + field.datatype)
    str.append("\" unit=\"" + field.unit)
    str.append("\">")
    str.append("<DESCRIPTION>" + field.description)
    str.append("</DESCRIPTION>")
    str.append("</FIELD>")
    w.write(str.toString())
}

fun writeRow(w: Writer, row: List<Any?>) {
    var str = StringBuilder("<TR>")
    for (col in row) {
        str.append("<TD>")
        str.append(col)
        str.append("</TD>")
    }
    str.append("</TR>")
    w.write(str.toString())
}

class TableMapper() : ObjectMapper() {

    override fun writeValue(w: Writer, entity: Any) {
        var str: StringBuilder
        if (entity is ErrorResponse) {
            val err = entity
            str = StringBuilder("<Error>")
            str.append("<Message>")
            str.append(err.message)
            str.append("<type>")
            str.append(err.type)
            str.append("<code>")
            str.append(err.code)
            str.append("<cause>")
            str.append(err.cause)
            w.write(str.toString())
            return
        }
        if (entity !is AsyncResponse) return
        val ar = entity
        var fields: List<ColumnMetadata> = ar.metadata.columns
        var rowIterator: Iterator<List<Any?>> = ar.results
        str = StringBuilder("<?xml version=\"1.0\"?>")
        str.append("<VOTABLE version=\"1.3\" xmlns=\"http://www.ivoa.net/xml/VOTable/v1.3\">")
        str.append("<RESOURCE name=\"Result for query: ")
        str.append(ar.queryId)
        str.append("\" type=\"meta\">")
        str.append("<TABLE name=\"")
        str.append("") // FIXME:
        str.append("\">")
        str.append("<DESCRIPTION>")
        str.append("Query Results")
        str.append("</DESCRIPTION>")
        w.write(str.toString())
        for (field in fields ) {
            writeField(w, field)
        }
        w.write("<DATA><TABLEDATA>")
        // process the data rows
        for (row in rowIterator) {
            writeRow(w, row)
        }
        str = StringBuilder("</TABLEDATA></DATA>")
        str.append("</TABLE>")
        str.append("</RESOURCE>")
        str.append("</VOTABLE>")
        w.write(str.toString())
        w.close()
    }
}
