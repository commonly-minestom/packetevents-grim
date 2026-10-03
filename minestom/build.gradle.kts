import org.gradle.api.tasks.testing.logging.TestExceptionFormat

plugins {
    packetevents.`shadow-conventions`
    packetevents.`library-conventions`
}

repositories {
    mavenCentral()
}

dependencies {
    compileOnly(libs.minestom)
    compileOnly(libs.jctools)
    shadow(project(":api", "shadow"))

    testImplementation(libs.minestom)
    testImplementation(libs.jctools)
    testImplementation(testlibs.bundles.junit)
    testRuntimeOnly(testlibs.slf4j)
}

configure<JavaPluginExtension> {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

tasks {
    withType<JavaCompile> {
        options.release = 25
    }

    test {
        useJUnitPlatform()
        testLogging {
            exceptionFormat = TestExceptionFormat.FULL
        }
    }
}
