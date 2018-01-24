buildscript {
    repositories {
        mavenCentral()
        jcenter()
    }
    dependencies {
        classpath("org.jetbrains.kotlin:kotlin-gradle-plugin:1.2.20")
    }
}

repositories {
    mavenCentral()
}

group = "org.lsst.dax"
version = "1.0.0-SNAPSHOT"

apply {
    plugin("kotlin")
}

plugins {
    application
}

application {
    mainClassName = "org.lsst.dax.albuquery.JettyServer"
}

dependencies {
    compile(kotlin("stdlib" ))
    compile("org.glassfish.jersey.core:jersey-server:2.26")
    compile("org.glassfish.jersey.containers:jersey-container-netty-http:2.26")
    compile("org.glassfish.jersey.containers:jersey-container-jetty-http:2.26")
    compile("org.glassfish.jersey.media:jersey-media-json-jackson:2.26")
    compile("org.glassfish.jersey.media:jersey-media-jaxb:2.26")
    compile("org.glassfish.jersey.inject:jersey-hk2:2.26")
    compile("com.fasterxml.jackson.module:jackson-module-kotlin:2.9.2")
    compile("com.facebook.presto:presto-parser:0.192")
    compile("javax.xml.bind:jaxb-api:2.3.0")
    compile("javax.activation:activation:1.1.1")
    compile("org.jetbrains.kotlin:kotlin-stdlib-jre8:1.2.20")
    compile("org.jetbrains.kotlin:kotlin-reflect:1.2.20")
    compile("org.mariadb.jdbc:mariadb-java-client:2.2.1")


    runtime("ch.qos.logback:logback-classic:1.1.8")
    runtime("org.slf4j:jul-to-slf4j:1.7.21")
}
