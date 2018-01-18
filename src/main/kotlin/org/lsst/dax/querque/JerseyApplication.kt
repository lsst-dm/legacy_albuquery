package org.lsst.dax.querque

import javax.ws.rs.core.Application

class JerseyApplication: Application() {
    override fun getSingletons(): MutableSet<Any> {
        return mutableSetOf(UserResource())
    }
}