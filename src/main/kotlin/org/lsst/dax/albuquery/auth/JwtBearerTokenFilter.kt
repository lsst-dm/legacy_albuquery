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
class JwtBearerTokenFilter : ContainerRequestFilter {

    val AUTHORIZATION_HEADER = "Authorization"
    val VERIFIER: JWTVerifier

    init {
        val issuer = getDemoIssuer()
        val publicKey = getDemoPublicKey()
        val algorithm = Algorithm.RSA256(publicKey)
        VERIFIER = JWT.require(algorithm)
            .withIssuer(issuer)
            .build()
    }

    override fun filter(requestContext: ContainerRequestContext) {
        val authHeader = requestContext.getHeaderString(AUTHORIZATION_HEADER)
        val token = authHeader.split(" ")[1]
        var authenticated = false
        try {
            val jwt = VERIFIER.verify(token)
            requestContext.securityContext = JwtSecurityContext(jwt, requestContext.securityContext)
            authenticated = true
        } catch (exception: JWTVerificationException) {
            LOGGER.info("Error in authentication")
            LOGGER.info(exception.toString())
            throw WebApplicationException(exception.message, Response.Status.UNAUTHORIZED)
        }

        if (!authenticated) {
            throw WebApplicationException(Response.Status.UNAUTHORIZED)
        }
    }

    data class JwtSecurityContext(val jwt: DecodedJWT, val originalContext: SecurityContext) : SecurityContext {
        val principal: Principal

        init {
            principal = object : Principal {
                override fun getName(): String {
                    return jwt.subject
                }
            }
        }

        override fun isUserInRole(role: String?): Boolean {
            return role in jwt.claims
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

        // FIXME: These should be dynamic via JWKS or static
        private fun getDemoPublicKey(): RSAPublicKey {
            val keyE = "AQAB"
            val keyN = "uGDGTLXnqh3mfopjys6sFUBvFl3F4Qt6NEYphq_u_aBhtN1X9NEyb78uB_I1KjciJNGLIQU0ECsJiFx6qV1hR9xE1" +
                "dPyrS3bU92AVtnBrvzUtTU-aUZAmZQiuAC_rC0-z_TOQr6qJkkUgZtxR9n9op55ZBpRfZD5dzhkW4Dm146vfTKt0D4cIMoMN" +
                "JS5xQx9nibeB4E8hryZDW_fPeD0XZDcpByNyP0jFDYkxdUtQFvyRpz4WMZ4ejUfvW3gf4LRAfGZJtMnsZ7ZW4RfoQbhiXKMf" +
                "WeBEjQDiXh0r-KuZLykxhYJtpf7fTnPna753IzMgRMmW3F69iQn2LQN3LoSMw=="
            val kf = KeyFactory.getInstance("RSA")
            val modulus = BigInteger(1, Base64.decodeBase64(keyN))
            val exponent = BigInteger(1, Base64.decodeBase64(keyE))
            val key = kf.generatePublic(RSAPublicKeySpec(modulus, exponent))
            return key as RSAPublicKey
        }

        private fun getDemoIssuer(): String {
            return "https://demo.scitokens.org"
        }
    }
}
