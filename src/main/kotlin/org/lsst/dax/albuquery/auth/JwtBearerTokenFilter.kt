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

package org.lsst.dax.albuquery.auth

import javax.annotation.Priority
import javax.ws.rs.Priorities
import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.container.ContainerRequestFilter
import javax.ws.rs.ext.Provider
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response
import com.auth0.jwt.exceptions.JWTVerificationException
import com.auth0.jwt.JWT
import com.auth0.jwt.JWTVerifier
import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.DecodedJWT
import org.apache.commons.codec.binary.Base64
import org.slf4j.LoggerFactory
import java.math.BigInteger
import java.security.KeyFactory
import java.security.Principal
import java.security.interfaces.RSAPublicKey
import java.security.spec.RSAPublicKeySpec
import javax.ws.rs.core.SecurityContext

@Provider
@Priority(Priorities.AUTHENTICATION)
class JwtBearerTokenFilter(
    publicKey: RSAPublicKey,
    issuer: String? = null,
    audience: String? = null
) : ContainerRequestFilter {

    private val verifier: JWTVerifier

    init {
        // Public Key only
        val algorithm = Algorithm.RSA256(publicKey, null)
        val builder = JWT.require(algorithm).withIssuer(issuer)
        if (audience != null) {
            builder.withAudience(audience)
        }
        verifier = builder.build()
    }

    override fun filter(requestContext: ContainerRequestContext) {
        val authHeader = requestContext.getHeaderString(AUTHORIZATION_HEADER)
        if (authHeader.indexOf("Bearer") != 0) {
            throw WebApplicationException("No Bearer token found or header malformed", Response.Status.UNAUTHORIZED)
        }
        val token = authHeader.substring("Bearer".length).trim()
        try {
            val jwt = verifier.verify(token)
            requestContext.securityContext = JwtSecurityContext(jwt, requestContext.securityContext)
        } catch (exception: JWTVerificationException) {
            LOGGER.info("Error in authentication")
            LOGGER.info(exception.toString())
            throw WebApplicationException(exception.message, Response.Status.UNAUTHORIZED)
        }
    }

    data class JwtSecurityContext(
        val jwt: DecodedJWT,
        private val originalContext: SecurityContext
    ) : SecurityContext {
        val principal: Principal
        val groups = arrayListOf<String>()

        init {
            principal = object : Principal {
                override fun getName(): String {
                    return jwt.subject
                }
            }
            val maybeGroups = jwt.claims["groups"]
            if (maybeGroups != null && maybeGroups is List<*>) {
                maybeGroups.forEach {
                    if (it is String) {
                        groups.add(it)
                    }
                }
            }
        }

        override fun isUserInRole(role: String?): Boolean {
            return role in groups
        }

        override fun getAuthenticationScheme(): String {
            return originalContext.authenticationScheme
        }

        override fun getUserPrincipal(): Principal {
            return principal
        }

        override fun isSecure(): Boolean {
            return originalContext.isSecure
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(JwtBearerTokenFilter::class.java)
        val AUTHORIZATION_HEADER = "Authorization"

        fun getKey(n: String, e: String): RSAPublicKey {
            val kf = KeyFactory.getInstance("RSA")
            val modulus = BigInteger(1, Base64.decodeBase64(n))
            val exponent = BigInteger(1, Base64.decodeBase64(e))
            val key = kf.generatePublic(RSAPublicKeySpec(modulus, exponent))
            return key as RSAPublicKey
        }
    }
}
