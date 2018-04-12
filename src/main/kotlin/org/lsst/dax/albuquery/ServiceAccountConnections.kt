package org.lsst.dax.albuquery

import org.slf4j.LoggerFactory
import java.net.URI
import java.sql.Connection
import java.sql.DriverManager

class ServiceAccountConnections(serviceAccountCredentials: List<Credential>) {
    val hostPortMap: HashMap<String, Credential> = hashMapOf()

    init {
        for (credential in serviceAccountCredentials) {
            hostPortMap[credential.server + credential.port] = credential
        }
    }

    fun getConnection(connectionUri: URI): Connection {
        LOGGER.debug("Getting connection for URI: $connectionUri")
        // FIXME: Can't do much about this for now unless we store passwords in the db
        val uri = "jdbc:" + connectionUri
        val credential = hostPortMap[connectionUri.host + connectionUri.port]
        return DriverManager.getConnection(uri, credential?.username, credential?.password)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ServiceAccountConnections::class.java)
    }
}
