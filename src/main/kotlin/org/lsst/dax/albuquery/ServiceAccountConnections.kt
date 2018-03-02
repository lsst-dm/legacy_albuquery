package org.lsst.dax.albuquery

import java.net.URI
import java.sql.Connection
import java.sql.DriverManager

class ServiceAccountConnections(val serviceAccountCredentials: List<Credential>) {
    val hostPortMap: HashMap<String, Credential>

    init {
        hostPortMap = hashMapOf()
        for (credential in serviceAccountCredentials) {
            hostPortMap[credential.server + credential.port] = credential
        }
    }

    fun getConnection(connectionUri: URI): Connection {
        // FIXME: Can't do much about this for now unless we store passwords in the db
        val uri = "jdbc:" + connectionUri
        val credential = hostPortMap[connectionUri.host + connectionUri.port]
        return DriverManager.getConnection(uri, credential?.username, credential?.password)
    }
}
