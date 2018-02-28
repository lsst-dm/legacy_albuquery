package org.lsst.dax.albuquery

import com.fasterxml.jackson.annotation.JsonProperty
import io.dropwizard.Configuration
import io.dropwizard.db.DataSourceFactory

class AlbuqueryConfiguration(
    @JsonProperty("dax_metaserv_db")
    val DAX_METASERV_DB: DataSourceFactory,

    @JsonProperty("dax_db_default_user")
    val DAX_DB_DEFAULT_USER: String,

    @JsonProperty("dax_db_default_password")
    val DAX_DB_DEFAULT_PASSWORD: String

) : Configuration()
