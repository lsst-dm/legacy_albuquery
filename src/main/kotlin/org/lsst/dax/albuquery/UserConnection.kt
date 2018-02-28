package org.lsst.dax.albuquery

import java.sql.Connection
import java.sql.DriverManager

fun getConnection(connectionUri: String?): Connection {
    // FIXME: Can't do much about this for now unless we store passwords in the db
    val user = CONFIG?.DAX_DB_DEFAULT_USER
    val password = CONFIG?.DAX_DB_DEFAULT_PASSWORD
    return DriverManager.getConnection(connectionUri, user, password)
}
