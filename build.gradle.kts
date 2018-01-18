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
    mainClassName = "org.lsst.dax.querque.NettyServer"
}

dependencies {
    compile(kotlin("stdlib" ))
    compile("org.glassfish.jersey.core:jersey-server:2.26")
    compile("org.glassfish.jersey.containers:jersey-container-netty-http:2.26")
    compile("org.glassfish.jersey.media:jersey-media-json-jackson:2.26")
    compile("org.glassfish.jersey.inject:jersey-hk2:2.26")
    compile("com.fasterxml.jackson.module:jackson-module-kotlin:2.8.4")

    runtime("ch.qos.logback:logback-classic:1.1.8")
}
