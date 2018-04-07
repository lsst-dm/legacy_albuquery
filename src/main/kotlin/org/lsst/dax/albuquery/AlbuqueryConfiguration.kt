package org.lsst.dax.albuquery

import com.fasterxml.jackson.annotation.JsonProperty
import io.dropwizard.Configuration
import io.dropwizard.db.DataSourceFactory

data class Credential(
    val server: String,
    val port: Int,
    val username: String?,
    val password: String?
)

class AlbuqueryConfiguration(
    @JsonProperty("dax_metaserv_db")
    val DAX_METASERV_DB: DataSourceFactory,

    @JsonProperty("dax_password_store")
    val DAX_PASSWORD_STORE: List<Credential>,

    @JsonProperty("dax_base_path")
    var DAX_BASE_PATH: String?

) : Configuration()
